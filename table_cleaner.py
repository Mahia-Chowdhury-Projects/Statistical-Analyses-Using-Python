# CS104 Data cleaning functions
# Fall 2022

from datascience import *
import numpy as np

def has_type(T):
    """Return a function that indicates whether value can be converted
       to the type T."""
    def is_a_T(v):
        try:
            T(v)
            return True
        except ValueError:
            return False
    return is_a_T


def valid_rows_for_column(table, column_name, expected_type):
    """Given a table, and column name, and the type of data you expect
       in that column (eg: str, int, or float), this function returns
       a new copy of the table with the following changes:
    
         - Any rows with missing values in the given column are removed.
         - Any rows with values in the given column that are not of the 
           expected type are removed.
    """
    
    # Remove rows w/ nans.  Columns with floats are treated different, since nans are 
    #  converted in np.nan when a floating point column is created in read_table.
    if (table.column(column_name).dtype == np.float64):
        new_table = table.where(column_name, lambda x: not np.isnan(x))
        removed_rows = table.where(column_name, lambda x: np.isnan(x))
    else:
        new_table = table.where(column_name, are.not_equal_to('nan'))
        removed_rows = table.where(column_name, are.equal_to('nan'))

    # Remove rows w/ values that cannot be converted to the expected type.
    removed_rows.append(new_table.where(column_name, lambda x: not has_type(expected_type)(x)))
    new_table = new_table.where(column_name, has_type(expected_type))

    # Make sure all rows have values of the expected type.  (eg: convert str's to int's for an
    #   int column.)
    new_table = new_table.with_column(column_name, new_table.apply(expected_type, column_name))
    
    # Print removed rows for debugging.
    print('Removed', removed_rows.num_rows, 'bad row(s) for column', column_name)
    
    return new_table


def bad_rows_for_column(table, column_name, expected_type):
    """Given a table, and column name, and the type of data you expect
       in that column (eg: str, int, or float), this function returns
       a table with:
    
         - Any rows with missing values in the given column.
         - Any rows with values in the given column that are not of the 
           expected type.
    """    
    # Remove rows w/ nans.  Columns with floats are treated different, since nans are 
    #  converted in np.nan when a floating point column is created in read_table.
    if (table.column(column_name).dtype == np.float64):
        new_table = table.where(column_name, lambda x: not np.isnan(x))
        removed_rows = table.where(column_name, lambda x: np.isnan(x))
    else:
        new_table = table.where(column_name, are.not_equal_to('nan'))
        removed_rows = table.where(column_name, are.equal_to('nan'))

    # Remove rows w/ values that cannot be converted to the expected type.
    removed_rows.append(new_table.where(column_name, lambda x: not has_type(expected_type)(x)))
    
    return removed_rows

def replace_value_in_column(table, column_name, original_value, new_value):
    """
    Replace all occurences of the original_value with the new_value
    in the column_name column of table, returning the updated table.
    """
    col = table.column(column_name).copy()
    col[col == original_value] = new_value
    return table.with_columns(column_name, col)
