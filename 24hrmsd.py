import os
import fnmatch
import obspy
from obspy import read, Stream

######################################################
# 1. Load data and filter channels and year
######################################################

# filtering to only select channel and year for combinations
current_year = input("What year of data do you want to use? ")
channel_input = input("What channel do you want to combine? 1 => Z component; 2 => N component; 3 => E component: ")
station_input = input("What station data do you want to merge? 1 => OHU1; 2 => OHG1; 3 => OHW1: ")
if station_input == "1":
    station = "OHU1"
    print("Station OHU1 has been selected for merging")
elif station_input == "2":
    station = "OHG1"
    print("Station OHG1 has been selected for merging")
elif station_input == "3":
    station = "OHW1"
    print("Station OHW1 has been selected for merging")
else:
    print("Input not recognized, try again")

directory_path = r"N:\SeisSection\Noble\PAS2MSD"  # Current directory

# Get all entries (files and directories)
all_entries = os.listdir(directory_path)

# Filter to get only file names
file_names = [entry for entry in all_entries if os.path.isfile(os.path.join(directory_path, entry))]

#print(file_names)


######################################################
# 2. Function to find min / max day of data for combination purposes
######################################################
def find_minimum_day_entry(list):
    """
    Finds the minimum Julian day entry from a list of filenames in a given directory.

    Assumes filenames have the format YYYYJJJ... where:
    - YYYY is the year (characters 1-4, indices 0-3)
    - JJJ is the Julian day (characters 5-7, indices 4-6)

    Args:
        directory (str): The path to the directory containing the files.

    Returns:
        int or None: The minimum Julian day found, or None if no valid day
                     entries are found or the directory is empty/invalid.
    """
    day_entries = []

    for filename in list:
        # We expect filenames to be at least 7 characters long to extract YYYYJJJ
        if len(filename) >= 7:
            try:
                # Extract the Julian day part (characters 5, 6, 7 which are indices 4, 5, 6)
                day_str = filename[4:7]
                day = int(day_str)
                day_entries.append(day)
            except (ValueError, IndexError) as e:
                # Handle cases where:
                # - ValueError: the extracted substring is not a valid integer
                # - IndexError: the filename was shorter than expected (though len() check helps)
                print(f"Warning: Could not parse day from filename '{filename}': {e}")
                pass # Skip this file and continue
        else:
            print(f"Warning: Filename '{filename}' is too short to extract day entry.")

    # saving the minimum day / maximum day for loop
    min_day = min(day_entries)
    max_day = max(day_entries)
    return min_day, max_day


# finding minimum and max day of provided data using function above:
min_day, max_day = find_minimum_day_entry(file_names)


######################################################
# 3. Loop to combine 24 single hour files into one 24hr file
######################################################

for d in range(min_day, (max_day + 1)):
#for d in range(315,366):
        # f"..." makes it an f-string.
        # {d:03d} formats 'd' as a 3-digit number with leading zeros (e.g., 1 -> 001).
        # {h:02d} formats 'h' as a 2-digit number with leading zeros (e.g., 1 -> 01).
    
    pattern = f"{current_year}{d:03d}*_{station}__1_{channel_input}.msd"

    filtered_files = [f for f in file_names if fnmatch.fnmatch(f, pattern)]

        # Use the constructed pattern in fnmatch.fnmatch
        # This will find files that match the exact year, day, and hour,
        # followed by any characters, then the fixed suffix.
    day_hour_files = [f for f in filtered_files if fnmatch.fnmatch(f, pattern)]
    day_hour_files.sort()
        # Initialize an empty ObsPy Stream object to hold all traces for this day
    daily_stream = Stream()

    # Read each 60-minute file into the daily_stream
    for file in day_hour_files:
        full_file_path = os.path.join(directory_path, file)

        try:
            print(f"Attempting to read: {full_file_path}") # For debugging

                # Read the MiniSEED file. This returns an ObsPy Stream object.
            st = read(full_file_path)               
            daily_stream += st # Append the traces from this file to the daily stream
            print(f"  - Read {file} ({len(st)} traces)")
        except Exception as e:
            print(f"  - ERROR: Could not read {file}: {e}. Skipping this file.")

    if not daily_stream:
        print(f"No valid traces loaded for day {d:03d}. Skipping merge.")
        continue

    # Merge all traces in the daily_stream
    # method=0: Merge traces even if they overlap (first trace takes precedence)
    # fill_value=0: Fill gaps with zeros
    print(f"Merging {len(daily_stream)} traces for day {d:03d}...")
    try:
        daily_stream.merge(method=0, fill_value=0)
        print(f"Merge complete. Stream now has {len(daily_stream)} traces after merge.")

        # Define the output filename for the 24-hour file
        if channel_input == '1':
            output_filename = f"{station}.OH.--.HHZ.{current_year}.{d:03d}.msd"
        elif channel_input == '2':
            output_filename = f"{station}.OH.--.HHN.{current_year}.{d:03d}.msd"
        elif channel_input == '3':
            output_filename = f"{station}.OH.--.HHE.{current_year}.{d:03d}.msd"

######## Where you want to dump 24hr long files:
        output_directory_path = rf"N:\SeisSection\Noble\PAS2MSD\{station}"
        output_file_path = os.path.join(output_directory_path, output_filename)

        # Save the merged 24-hour MiniSEED file
        daily_stream.write(output_file_path, format="MSEED")
        print(f"Successfully saved 24-hour file: {output_filename}")
        print()
        print("----------------------------------------------")
        print()

    except Exception as e:
        print(f"ERROR: Failed to merge or save stream for day {d:03d}: {e}")
