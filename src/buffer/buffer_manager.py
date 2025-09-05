from pathlib import Path
from typing import List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


class EmailBufferManager:
    def __init__(self, batch_size: int, output_path: Path, use_streaming: bool = False):
        self.buffer = []
        self.batch_size = batch_size
        self.output_path = output_path
        self.schema = None
        self.use_streaming = use_streaming
        self.parquet_writer = None
        # Ensure the output directory exists
        self.output_path.parent.mkdir(exist_ok=True, parents=True)

    def add_emails(self, emails: List[dict]):
        """
        Adds a list of email dictionaries to the buffer.
        If the buffer exceeds the batch size, it triggers a flush.
        """
        self.buffer.extend(emails)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        """
        Writes the current buffer to the Parquet file and clears it.
        Crucially, it appends to the file if it already exists.
        """
        if not self.buffer:
            return

        df = pd.DataFrame(self.buffer)
        table = pa.Table.from_pandas(df)

        if self.use_streaming:
            # Use streaming writer for better performance
            if self.parquet_writer is None:
                self.schema = table.schema
                self.parquet_writer = pq.ParquetWriter(self.output_path, self.schema)

            self.parquet_writer.write_table(table)
        else:
            # Use read-combine-write approach
            if os.path.exists(self.output_path):
                # If the file exists, read existing data and append
                existing_table = pq.read_table(self.output_path)

                # Ensure schemas are compatible
                if self.schema is None:
                    self.schema = existing_table.schema

                # Combine tables
                combined_table = pa.concat_tables([existing_table, table])

                # Write the combined data back
                pq.write_table(combined_table, self.output_path)
            else:
                # If it's the first write, create the file
                self.schema = table.schema
                pq.write_table(table, self.output_path)

        # Clear the buffer after writing
        records_written = len(df)
        self.buffer.clear()
        print(f"Flushed {records_written} records to {self.output_path}")

    def finalize(self):
        """
        Flushes any remaining data in the buffer.
        """
        self.flush()
        if self.parquet_writer:
            self.parquet_writer.close()
            self.parquet_writer = None

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures final flush"""
        self.finalize()