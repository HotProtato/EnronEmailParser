import pandas as pd
from pathlib import Path
from collections import defaultdict

def run(paths: dict):
    """
    This script processes the group_table after user deduplication.
    It performs three main tasks:
    1. Updates group memberships by removing deleted users and remapping merged users.
    2. Identifies and deduplicates groups that have become identical after the updates.
    3. Generates a cleaned group table and a remap table for updating the email_table.
    """
    # --- 1. Load Data ---
    print("Loading data...")
    GROUP_TABLE_PATH = paths["group_table"]
    USER_MAP_TABLE_PATH = paths["user_map_table"]
    TO_DELETE_TABLE_PATH = paths["to_delete_table"]
    GROUPS_UPDATED_PATH = paths["groups_updated"]
    GROUP_REMAP_PATH = paths["group_remap"]
    
    group_df = pd.read_parquet(GROUP_TABLE_PATH)
    user_map_df = pd.read_parquet(USER_MAP_TABLE_PATH)
    to_delete_df = pd.read_parquet(TO_DELETE_TABLE_PATH)

    # --- 2. Create Lookup Maps for Efficiency ---
    print("Creating lookup maps...")
    to_delete_set = set(to_delete_df["user_id"])
    user_remap_dict = pd.Series(user_map_df.parent_id.values, index=user_map_df.child_id).to_dict()

    # --- 3. Update Group Memberships ---
    print("Updating group memberships...")
    
    def update_user_list(user_ids):
        """
        Applies deletions and remaps to a list of user IDs from a group.
        Returns a frozenset of the updated user IDs for easy comparison.
        """
        updated_users = set()
        for user_id in user_ids:
            if user_id in to_delete_set:
                continue  # Skip deleted users
            
            # If the user_id is a 'child', replace it with the 'parent'
            mapped_id = user_remap_dict.get(user_id, user_id)
            updated_users.add(mapped_id)
        
        return frozenset(updated_users)

    # Apply the update function to the 'user_ids' column
    group_df["updated_user_ids"] = group_df["user_ids"].apply(update_user_list)

    # --- 4. Deduplicate Groups ---
    print("Deduplicating groups...")
    
    seen_groups = {}  # Maps a frozenset of user_ids to the first group_id seen
    group_remap = {}  # Maps a redundant/empty group_id to its canonical group_id or -1
    
    for index, row in group_df.iterrows():
        group_id = row["group_id"]
        user_set = row["updated_user_ids"]
        
        # Check if the group is now empty.
        if not user_set:
            group_remap[group_id] = -1 # Mark for deletion
            continue

        if user_set in seen_groups:
            canonical_group_id = seen_groups[user_set]
            if group_id != canonical_group_id:
                group_remap[group_id] = canonical_group_id
        else:
            seen_groups[user_set] = group_id
            
    # --- 5. Generate Final Outputs ---
    print("Generating final output files...")
    
    # Create a DataFrame for the group remapping
    group_remap_df = pd.DataFrame(list(group_remap.items()), columns=["old_group_id", "new_group_id"])
    
    # Get the list of all group IDs that will be replaced or deleted
    obsolete_group_ids = set(group_remap.keys())
    
    # Create the updated group table by dropping the obsolete groups
    updated_group_df = group_df[~group_df["group_id"].isin(obsolete_group_ids)].copy()
    
    updated_group_df.drop(columns=["user_ids"], inplace=True)
    updated_group_df.rename(columns={"updated_user_ids": "user_ids"}, inplace=True)
    # Convert frozenset back to list for Parquet compatibility
    updated_group_df["user_ids"] = updated_group_df["user_ids"].apply(list)
    
    # Write to Parquet
    updated_group_df.to_parquet(GROUPS_UPDATED_PATH, index=False)
    group_remap_df.to_parquet(GROUP_REMAP_PATH, index=False)
    
    print(f"Processing complete.")
    print(f"Saved updated groups to: {GROUPS_UPDATED_PATH}")
    print(f"Saved group remap table to: {GROUP_REMAP_PATH}")
    print(f"{len(obsolete_group_ids)} groups were identified as duplicates or empty and will be remapped.")

