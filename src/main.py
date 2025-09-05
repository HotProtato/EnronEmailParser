import functools
from multiprocessing import Pool, Manager
from pathlib import Path
import os
import re
import hashlib
from typing import Set, List, Dict
import traceback

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from email_pipeline.pipeline import EmailPipeline
from src.buffer.buffer_manager import EmailBufferManager
from src.data_object.processed_email import ProcessedEmail
from user_pipeline.pipeline import UserPipeline
from group_pipeline.pipeline import GroupPipeline
from data_object.user_profile import UserProfile
from tqdm import tqdm
import quopri

EMAIL_TABLE_OUTPUT_PATH = Path(__file__).parent.parent / "output" / "email_table.parquet"
USER_TABLE_OUTPUT_PATH = Path(__file__).parent.parent / "output" / "user_table.parquet"
GROUP_TABLE_OUTPUT_PATH = Path(__file__).parent.parent / "output" / "group_table.parquet"

def parse_and_canonicalize(file_path) -> str | None:
    try:
        with open("\\\\?\\" + str(file_path.resolve()), "rb") as file:
            email = file.read().decode()

            canon_filter = r"(X-Folder:|X-Origin:|X-FileName:|Message-ID:).*\n"

            canon_email = re.sub(canon_filter, "", email)
            canon_email_encoded = quopri.decodestring(canon_email)
            canon_email = decode_str(canon_email_encoded)
            return canon_email
    except Exception as e:
        print(f"Error while parsing {file_path}. Details: {e}")
        print(traceback.format_exc())
        return None

def process_single_file(file_path, message_cache, file_cache, cache_lock) -> List[ProcessedEmail] | None:
    try:
        # Parse and canonicalize (no shared state needed)
        canon_email = parse_and_canonicalize(file_path)
        if not canon_email:
            return None

        hash_obj = hashlib.md5()
        hash_obj.update(canon_email.encode("utf-8"))
        hash_obj = hash_obj.hexdigest()
        with cache_lock:
            if hash_obj in file_cache:
                return None
            file_cache[hash_obj] = True

        # Process with shared cache (lock managed internally)
        email_manager = EmailPipeline()  # Stateless now!
        processed_emails = email_manager.process_file_contents(
            canon_email,
            message_cache,
            cache_lock
        )
        return processed_emails
    except Exception as e:
        print(f"Exception caught. Details: {e}")
        return None


def run():
    root = Path(__file__).parent.parent / "input" / "maildir"
    _remove_files(root)

    if os.path.exists(EMAIL_TABLE_OUTPUT_PATH):
        os.remove(EMAIL_TABLE_OUTPUT_PATH)

    file_paths = list(root.rglob("*."))

    user_manager = UserPipeline()
    group_manager = GroupPipeline()
    email_buffer_manager = EmailBufferManager(batch_size=1000, output_path=EMAIL_TABLE_OUTPUT_PATH)

    with Manager() as manager:
        file_cache = manager.dict()
        msg_cache = manager.dict()
        lock = manager.Lock()

        worker = functools.partial(process_single_file, message_cache=msg_cache, file_cache=file_cache, cache_lock=lock)

        with Pool(processes=None) as pool:
            results = pool.imap_unordered(worker, file_paths, chunksize=1000)

            for processed_emails in tqdm(results, total=len(file_paths)):
                if not processed_emails:
                    continue
                # Accounting for aliases that use space deliminators instead of ", " to reformat for user pipeline
                # ingestion.

                for processed_email in processed_emails:
                    users: Set[int] = set()
                    for alias in processed_email.aliases:
                        if not alias or not alias.strip():
                            continue
                        if alias.count(",") > 3:
                            # Assume spaces are the deliminators, as for a single alias to have many , means the regex failed.
                            for alias_ in alias.split(", "):
                                if "@" in alias_:
                                    users.add(user_manager.get_user_id(alias_))
                                    continue
                                users.add(user_manager.get_user_id(alias_.replace(" ", ", ")))
                        else:
                            _id = user_manager.get_user_id(alias)
                            users.add(_id)
                    group_id = group_manager.get_group_id(users)
                    sender_id = user_manager.get_user_id_from_set(processed_email.sender) if processed_email.sender else -1

                    email_data = {
                        "email_hash": processed_email.email_hash,
                        "group_id": group_id,
                        "subject": processed_email.subject,
                        "date": processed_email.date,
                        "norm_date": processed_email.norm_date,
                        "sender_id": sender_id
                    }
                    email_buffer_manager.add_emails([email_data])

    # --- Final Flush ---
    # Ensure any remaining emails in the buffer are written to the file.
    print("Flushing final email batch...")
    email_buffer_manager.flush()

    _write_users_to_parquet(user_manager.users, USER_TABLE_OUTPUT_PATH)
    _write_groups_to_parquet(group_manager.group_cache, GROUP_TABLE_OUTPUT_PATH)

def decode_str(encoding: bytes) -> str | None:
    try:
        return encoding.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return encoding.decode("latin-1")
        except UnicodeDecodeError:
            print("Warning: Failed to decode with common encodings. Replacing invalid characters.")
            decoded_bytes = quopri.decodestring(encoding)
            try:
                return decoded_bytes.decode("utf-8", errors="replace")
            except UnicodeDecodeError:
                return None
    except Exception as e:
        raise ValueError(f"Failed to decode quoted-printable string: {e}")

def _write_batch_to_parquet(data_buffer: List[Dict], file_path: Path):
    """
    Writes a batch of processed email data to a Parquet file.
    Appends to the file if it already exists, otherwise creates it.
    """
    if not data_buffer:
        return

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(data_buffer)

    # Check if the file already exists
    if os.path.exists(file_path):
        # If it exists, append the data
        table = pa.Table.from_pandas(df, preserve_index=False)
        with pq.ParquetWriter(file_path, table.schema, compression='snappy') as writer:
            writer.write_table(table)
    else:
        # If it doesn't exist, create a new file
        df.to_parquet(file_path, engine='pyarrow', index=False)

def _write_groups_to_parquet(groups: Dict[frozenset, int], file_path: Path):
    """
    Writes the group table to a Parquet file.
    """
    group_data = []
    for users, group_id in groups.items():
        group_data.append({
            "group_id": group_id,
            "user_ids": list(users)
        })
    df = pd.DataFrame(group_data)
    df.to_parquet(file_path, engine='pyarrow', index=False)
    print(f"Wrote group table with {len(group_data)} records to {file_path}")

def _write_users_to_parquet(users: Dict[int, UserProfile], file_path: Path):
    """
    Writes the user table to a Parquet file.
    """
    user_data = []
    for user_id, profile in users.items():
        user_data.append({
            "user_id": user_id,
            "first_name": profile.first_name,
            "last_name": profile.last_name,
            "generated_aliases": list(profile.generated_aliases),
            "aliases": list(profile.aliases)
        })
    df = pd.DataFrame(user_data)
    df.to_parquet(file_path, engine='pyarrow', index=False)
    print(f"Wrote user table with {len(user_data)} records to {file_path}")


def _remove_files(root_path: Path):
    """
    Removes specific files from the dataset due to privacy concerns.
    """
    to_remove = [
        "maildir/skilling-j/1584.",
        "maildir/gay-r/all_documents/12.",
        "maildir/gay-r/all_documents/206.",
        "maildir/gay-r/all_documents/74.",
        "maildir/gay-r/sent/12.",
        "maildir/gay-r/sent/205.",
        "maildir/gay-r/sent/74.",
        "maildir/richey-c/sent_items/15.",
        "maildir/richey-c/sent_items/2.",
        "maildir/richey-c/sent_items/20.",
        "maildir/richey-c/sent_items/3.",
        "maildir/richey-c/sent_items/32.",
        "maildir/richey-c/sent_items/36.",
        "maildir/richey-c/sent_items/4.",
        "maildir/richey-c/sent_items/45.",
        "maildir/richey-c/sent_items/5.",
        "maildir/richey-c/sent_items/6.",
        "maildir/richey-c/sent_items/7.",
        "maildir/richey-c/inbox/10.",
        "maildir/richey-c/inbox/11.",
        "maildir/richey-c/inbox/13.",
        "maildir/richey-c/inbox/14.",
        "maildir/richey-c/inbox/15.",
        "maildir/richey-c/inbox/16.",
        "maildir/richey-c/inbox/17.",
        "maildir/richey-c/inbox/2.",
        "maildir/richey-c/inbox/33.",
        "maildir/richey-c/inbox/34.",
        "maildir/richey-c/inbox/44.",
        "maildir/richey-c/inbox/45."
    ]
    for item in to_remove:
        # Construct the full path
        path_to_remove = root_path / item

        # Use the UNC path prefix for Windows to handle long/non-standard paths
        full_path_str = "\\\\?\\" + str(path_to_remove.resolve())

        if os.path.exists(full_path_str):
            os.remove(full_path_str)
            print(f"Removed file: {full_path_str}")
        else:
            print(f"File not found, skipping: {full_path_str}")

if __name__ == "__main__":
    run()