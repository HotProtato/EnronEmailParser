import pandas as pd
from pathlib import Path

def run(paths: dict):
    """
    This is the final script in the post-processing pipeline.
    1. It updates the sender_id field based on user merges.
    2. It updates the email_table using the group_remap table.
    3. It removes rows corresponding to deleted (empty) groups.
    4. It drops duplicate (email_hash, group_id) rows that are created during remapping.
    5. It generates the final, normalized junction tables.
    """
    # --- 1. Load Data ---
    print("Loading data...")
    email_df = pd.read_parquet(paths["email_table"])
    group_remap_df = pd.read_parquet(paths["group_remap"])
    groups_updated_df = pd.read_parquet(paths["groups_updated"])
    user_map_df = pd.read_parquet(paths["user_map_table"])

    # --- 2. Apply User Remapping to sender_id ---
    print("Applying user remapping to the 'sender_id' field...")
    user_remap_dict = pd.Series(user_map_df.parent_id.values, index=user_map_df.child_id).to_dict()
    
    email_df["sender_id"] = email_df["sender_id"].map(user_remap_dict).fillna(email_df["sender_id"])
    email_df["sender_id"] = email_df["sender_id"].astype(int)

    # --- 3. Apply Group Remapping ---
    print("Applying group remapping to the email table...")
    group_remap_dict = pd.Series(group_remap_df.new_group_id.values, index=group_remap_df.old_group_id).to_dict()
    email_df["group_id"] = email_df["group_id"].map(group_remap_dict).fillna(email_df["group_id"]).astype(int)

    # --- 4. Remove Deleted Groups ---
    initial_rows = len(email_df)
    email_df = email_df[email_df["group_id"] != -1]
    rows_removed = initial_rows - len(email_df)
    print(f"Removed {rows_removed} rows corresponding to empty/deleted groups.")

    # --- 5. Drop Duplicate Rows ---
    print("Dropping duplicate (email_hash, group_id) rows...")
    initial_rows = len(email_df)
    email_df = email_df.drop_duplicates(subset=["email_hash", "group_id"])
    rows_removed = initial_rows - len(email_df)
    print(f"Removed {rows_removed} duplicate rows after group remapping.")

    # --- 6. Save Final Tables ---
    print("Saving final, cleaned tables...")
    
    # Save the final email table
    email_df.to_parquet(paths["final_email_table"], index=False)
    print(f"  - Saved updated email table to: {paths['final_email_table']}")

    # Create and save the email-to-group junction table
    email_group_junction_df = email_df[["email_hash", "group_id"]].copy().drop_duplicates()
    email_group_junction_df.to_parquet(paths["email_group_junction"], index=False)
    print(f"  - Saved email-group junction table to: {paths['email_group_junction']}")

    # Create and save the email-to-user junction table
    junction_df = pd.merge(email_df, groups_updated_df, on="group_id", how="inner")
    junction_df = junction_df.explode("user_ids")
    junction_df = junction_df[["email_hash", "user_ids"]].rename(columns={"user_ids": "user_id"})
    junction_df.drop_duplicates(inplace=True)
    junction_df.to_parquet(paths["email_user_junction"], index=False)
    print(f"  - Saved email-user junction table to: {paths['email_user_junction']}")

    print("Post-processing complete.")


