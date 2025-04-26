import pandas as pd


def mask_loan_number(loan_no, visible_digits=6, total_length=10):
    """
    Mask the loan number to be exactly 'total_length' digits long,
    while keeping the last 'visible_digits' digits visible.
    """
    loan_no = str(loan_no)

    if len(loan_no) < total_length:
        masked_part = "x" * (total_length - visible_digits)
        return masked_part + loan_no[-visible_digits:]

    return "x" * (total_length - visible_digits) + loan_no[-visible_digits:]


def process_dataframe(file_path, selected_column, desired_columns=None):
    df = pd.read_excel(file_path)

    # Log input columns for debugging
    print(f"Input file columns: {list(df.columns)}")

    if selected_column not in df.columns:
        raise ValueError(f"Column '{selected_column}' not found in the file!")

    # Rename the selected column to 'LoanNo/CC'
    df.rename(columns={selected_column: "LoanNo/CC"}, inplace=True)

    # Create a new column 'Masked_LoanNo/CC' with masked values
    df["Masked_LoanNo/CC"] = df["LoanNo/CC"].apply(mask_loan_number)

    # Check if 'assignedStatus' column exists; if not, add it
    if "assignedStatus" not in df.columns:
        df["assignedStatus"] = "unAssigned0"
    else:
        df["assignedStatus"] = df["assignedStatus"].fillna("unAssigned0")

    # Ensure all desired columns are in the output DataFrame
    if desired_columns:
        # Create a new DataFrame with all desired columns
        output_df = pd.DataFrame(index=df.index)
        for col in desired_columns:
            if col in df.columns:
                output_df[col] = df[col]
            else:
                output_df[col] = pd.NA  # Fill missing columns with NaN
                print(f"Column '{col}' not found in input file, filled with NaN")

        # Log the columns included in the output
        print(f"Output file columns: {list(output_df.columns)}")

        df = output_df

    # Save the updated DataFrame to a new file
    output_filename = file_path.replace(".xlsx", "_withMaskAndAssignedStatus.xlsx")
    df.to_excel(output_filename, index=False)
    return output_filename
