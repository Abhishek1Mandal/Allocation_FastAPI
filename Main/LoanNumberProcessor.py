import pandas as pd
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def mask_loan_number(loan_no, visible_digits=6, total_length=10):
    """
    Mask the loan number to show the last 'visible_digits' digits/characters, with 'x' for the rest,
    ensuring a total length of 'total_length'.
    """
    # Convert to string and strip whitespace
    loan_no = str(loan_no).strip()

    # Clean float artifacts (e.g., '.0')
    if loan_no.endswith(".0"):
        loan_no = loan_no[:-2]

    # Log the input for debugging
    logger.debug(f"Input loan number: {loan_no}")

    # If input is empty or too short, pad with zeros or handle as needed
    if not loan_no:
        logger.warning("Empty loan number")
        return "x" * total_length  # Fully masked for empty input

    # Take the last 'total_length' characters, padding with zeros if needed
    trimmed_loan = (
        loan_no[-total_length:]
        if len(loan_no) >= total_length
        else "0" * (total_length - len(loan_no)) + loan_no
    )

    # Create masked and visible parts
    masked_part = "x" * (total_length - visible_digits)
    visible_part = trimmed_loan[-visible_digits:]

    result = masked_part + visible_part
    logger.debug(f"Masked result: {result}")

    return result


def process_dataframe(file_path, selected_column="LoanNo/CC", desired_columns=None):
    """
    Process an Excel file to mask loan numbers in the specified column, add assignedStatus,
    and filter to desired columns. The selected column is renamed to 'LoanNo/CC'.
    """
    try:
        # Read the Excel file, forcing the selected column to be a string
        df = pd.read_excel(file_path, dtype={selected_column: str})

        # Check if the selected column exists
        if selected_column not in df.columns:
            logger.error(f"Column '{selected_column}' not found in file")
            raise ValueError(f"Column '{selected_column}' not found in the file!")

        logger.info(f"Input file columns: {list(df.columns)}")

        # Rename the selected column to LoanNo/CC
        df = df.rename(columns={selected_column: "LoanNo/CC"})

        # Clean any float artifacts in the LoanNo/CC column
        df["LoanNo/CC"] = (
            df["LoanNo/CC"].astype(str).str.replace(r"\.0$", "", regex=True)
        )

        # Debug: Log raw values
        logger.debug(f"Raw LoanNo/CC values: {df['LoanNo/CC'].head().tolist()}")

        # Apply the masking to a new column
        df["Masked_LoanNo/CC"] = df["LoanNo/CC"].apply(mask_loan_number)

        # Debug: Log masked values
        logger.debug(
            f"Masked_LoanNo/CC values: {df['Masked_LoanNo/CC'].head().tolist()}"
        )

        # Add or update assignedStatus column
        if "assignedStatus" not in df.columns:
            df["assignedStatus"] = "unAssigned0"
        else:
            df["assignedStatus"] = df["assignedStatus"].fillna("unAssigned0")

        # Filter to desired columns if provided
        if desired_columns:
            try:
                # Ensure desired_columns is a list
                if isinstance(desired_columns, str):
                    desired_columns = json.loads(desired_columns)

                # Create a new DataFrame with desired columns
                output_df = pd.DataFrame(index=df.index)
                for col in desired_columns:
                    if col in df.columns:
                        output_df[col] = df[col]
                    else:
                        output_df[col] = pd.NA
                        logger.warning(
                            f"Column '{col}' not found in input file, filled with NA"
                        )

                # Ensure Masked_LoanNo/CC and assignedStatus are included
                if "Masked_LoanNo/CC" not in desired_columns:
                    output_df["Masked_LoanNo/CC"] = df["Masked_LoanNo/CC"]
                if "assignedStatus" not in desired_columns:
                    output_df["assignedStatus"] = df["assignedStatus"]

                df = output_df
                logger.info(f"Output file columns: {list(df.columns)}")
            except json.JSONDecodeError:
                logger.error("Invalid JSON format for desired_columns")
                raise ValueError("Invalid JSON format for desired_columns")

        # Save the result to a new file
        output_filename = file_path.replace(".xlsx", "_withMaskAndAssignedStatus.xlsx")
        df.to_excel(output_filename, index=False)
        logger.info(f"Output file saved: {output_filename}")

        return output_filename

    except Exception as e:
        logger.error(f"Error processing DataFrame: {str(e)}")
        raise