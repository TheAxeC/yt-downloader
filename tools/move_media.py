import os
import shutil
import argparse

def process_roms(base_path):
    roms_path = os.path.join(base_path, 'roms')
    media_path = os.path.join(base_path, 'media')
    gamelist_path = os.path.join(base_path, 'gamelists')

    if not os.path.isdir(roms_path):
        print(f"Error: {roms_path} does not exist or is not a directory.")
        return

    for dir_name in os.listdir(roms_path):
        dir_path = os.path.join(roms_path, dir_name)
        if not os.path.isdir(dir_path):
            continue  # Skip files, only process directories

        dest_dir = os.path.join(media_path, dir_name)
        os.makedirs(dest_dir, exist_ok=True)

        dest_dir_gamelist = os.path.join(gamelist_path, dir_name)
        os.makedirs(dest_dir_gamelist, exist_ok=True)

        # Copy gamelist.xml if exists
        gamelist_file = os.path.join(dir_path, 'gamelist.xml')
        if os.path.isfile(gamelist_file):
            shutil.copy2(gamelist_file, os.path.join(dest_dir_gamelist, 'gamelist.xml'))
            print(f"Copied gamelist.xml for {dir_name}")

        # Copy media folder if exists
        media_folder = os.path.join(dir_path, 'media')
        if os.path.isdir(media_folder):
            shutil.copytree(media_folder, dest_dir, dirs_exist_ok=True)
            print(f"Copied media for {dir_name}")

def main():
    parser = argparse.ArgumentParser(description="Copy gamelists and media folders from roms to media directory.")
    parser.add_argument('path', help="Base path where 'roms' directory is located.")
    args = parser.parse_args()

    process_roms(args.path)

if __name__ == "__main__":
    main()
