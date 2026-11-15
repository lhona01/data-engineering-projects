import pandas as pd
import yaml

def load_rules(file):
    with open(file) as f:
        return yaml.safe_load(f)

# Validate (ensure data is in the correct format)
def validate_data(df, rules):
    accepted_df = df.copy()
    rejected_df = df.iloc[0:0].copy()

    def reject(mask):
        nonlocal accepted_df, rejected_df
        rejected_df = pd.concat([rejected_df, accepted_df[mask]], ignore_index=True)
        accepted_df = accepted_df[~mask]

    # required columns must not be null
    for col in (rules.get('required_columns') or []):
        reject(accepted_df[col].isna())

    # numeric minimums
    for col, minimum in (rules.get('min_values') or {}).items():
        nums = pd.to_numeric(accepted_df[col], errors='coerce')
        reject(nums < minimum)

    # date must parse
    for col in (rules.get('date_columns') or []):
        parsed = pd.to_datetime(accepted_df[col], errors='coerce')
        reject(parsed.isna())
        accepted_df = accepted_df.assign(**{col: parsed.loc[accepted_df.index]})

    return accepted_df, rejected_df