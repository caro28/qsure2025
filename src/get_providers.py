import pandas as pd


def get_providers(filein, fileout):
    df = pd.read_csv(filein, dtype=str)
    providers_npi_id = df[['Covered_Recipient_Profile_ID', 'Covered_Recipient_NPI']].copy()
    # fill na with empty string
    providers_npi_id = providers_npi_id.fillna('')
    # save to csv
    providers_npi_id.to_csv(fileout, index=False)

def main():
    filein = "data/reference/OP_CVRD_RCPNT_PRFL_SPLMTL_P01302025_01212025.csv"
    fileout = "data/reference/providers_npis_ids.csv"
    get_providers(filein, fileout)



if __name__ == "__main__":
    main()

    