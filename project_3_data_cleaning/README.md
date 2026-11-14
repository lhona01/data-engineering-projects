## What pipeline does
- **Extract:** [extracts data from csv file]
- **Transform:** [handles null values in the table]
- **Validate:** [validates correct data (range, date) is in the specified columns of the table]

# What i learned?
1. New concepts
- Cleaning and validating data are two different concept/things
- using yaml file to set up data validation rules so the validation method is applicable to multiple types of tables
- Storing rejected rows in different file for further interogation instead of deleting them

2. Problems I ran into
- storing rejected rows with all its data intact was a little tricky using pandas (errors="coerce") since that set the rejection cells to null
- refactoring validate_data method to make it applicable via a yaml file was tricky because of unfamiliar syntax.