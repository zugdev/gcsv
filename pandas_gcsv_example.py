import pandas_gcsv

# Read a GCSV file into a pandas df
print("Reading GCSV file...")
df = pandas_gcsv.read_gcsv('not_too_big.gcsv')
print("GCSV file read.")

# Display the df's head
print(df.head())

# Modify all values in the 'Column1' column to uppercase
print("Modifying 'Column1' column to uppercase...")
df['Column1'] = df['Column1'].str.upper()
print("Column modified.")

# Display the df's head
print(df.head())

# Write the modified df to a new GCSV file
print("Writing modified df to GCSV file...")
pandas_gcsv.to_gcsv(df, 'modified.gcsv')
print("GCSV file written.")