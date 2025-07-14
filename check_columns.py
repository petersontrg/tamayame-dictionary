import pandas as pd

# Load the Excel file
df = pd.read_excel("Davis_simplified.xlsx")

# Print the actual column names in the file
print("Actual column headers:")
print(df.columns.tolist())
