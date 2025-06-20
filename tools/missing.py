import os
import argparse

def find_and_print_files(root_path):
    for dirpath, dirnames, filenames in os.walk(root_path):
        for filename in filenames:
            if filename == "gamelist.Missing.Serial.txt":
                full_path = os.path.join(dirpath, filename)
                print(f"\n--- Contents of: {full_path} ---")
                try:
                    with open(full_path, 'r', encoding='utf-8') as file:
                        print(file.read())
                except Exception as e:
                    print(f"Error reading {full_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Recursively find and display gamelist.Missing.Serial.txt files")
    parser.add_argument("path", help="Root directory to process")
    args = parser.parse_args()

    find_and_print_files(args.path)

if __name__ == "__main__":
    main()
