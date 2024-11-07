import pandas as pd
import numpy as np

# %%
##############################################################################
# ====================        CELL TIERING          ==========================
##############################################################################


def cell_tiering(df_samples, anode_metrics, radiograph_metrics, mass_metrics):
    try:
        df_samples = (
            df_samples.merge(anode_metrics, on="US_id", how="left")
            # .merge(ultrasound_metrics, on="US_id", how="left")
            .merge(radiograph_metrics, on="US_id", how="left").merge(
                mass_metrics[
                    [
                        "US_id",
                        "US ESS mass",
                        "CS ESS mass",
                        "CS to US mass loss",
                        "DP00 mass",
                        "DP01 mass",
                    ]
                ],
                on="US_id",
                how="left",
            )
        )

        if "ultrasound_alpha2_tier" not in df_samples.columns:
            df_samples["ultrasound_alpha2_tier"] = np.nan
        if "radiograph_tier" not in df_samples.columns:
            df_samples["radiograph_tier"] = np.nan
        if "alpha2-tier" not in df_samples.columns:
            df_samples["alpha2-tier"] = np.nan
        if "US ESS mass" not in df_samples.columns:
            df_samples["US ESS mass"] = np.nan

        # Calculate cell tier
        df_samples["cell_tier"] = 1
        df_samples.loc[
            (df_samples["alpha2-tier"] == 2)
            | (df_samples["ultrasound_alpha2_tier"] == 2)
            | (df_samples["radiograph_tier"] == 2)
            | (df_samples["US ESS mass"] < 450),
            "cell_tier",
        ] = 2
        df_samples.loc[
            (df_samples["alpha2-tier"] == 3)
            | (df_samples["ultrasound_alpha2_tier"] == 3)
            | (df_samples["radiograph_tier"] == 3)
            | (df_samples["US ESS mass"] < 410),
            "cell_tier",
        ] = 3

        # Anode quality yield
        df_samples["Anode Count"] = (
            (df_samples["Yield Count"] == 1) & df_samples["alpha2-tier"].notna()
        ).astype(int)
        df_samples["Anode Yield Count"] = (
            np.where(df_samples["alpha2-tier"].astype(float) < 3, 1, 0)
            * df_samples["Anode Count"]
        )
        df_samples["Anode Yield Tier 1 Count"] = (
            np.where(df_samples["alpha2-tier"].astype(float) < 2, 1, 0)
            * df_samples["Anode Count"]
        )

        # Ultrasound defects yield
        df_samples["Ultrasound Count"] = (
            (df_samples["Yield Count"] == 1)
            & df_samples["ultrasound_alpha2_tier"].notna()
        ).astype(int)
        df_samples["Ultrasound Yield Count"] = (
            np.where(df_samples["ultrasound_alpha2_tier"].astype(float) < 3, 1, 0)
            * df_samples["Ultrasound Count"]
        )
        df_samples["Ultrasound Yield Tier 1 Count"] = (
            np.where(df_samples["ultrasound_alpha2_tier"].astype(float) < 2, 1, 0)
            * df_samples["Ultrasound Count"]
        )

        # Radiograph misalignment yield
        df_samples["Radiograph Count"] = (
            (df_samples["Yield Count"] == 1) & df_samples["radiograph_tier"].notna()
        ).astype(int)
        df_samples["Radiograph Yield Count"] = (
            np.where(df_samples["radiograph_tier"].astype(float) < 3, 1, 0)
            * df_samples["Radiograph Count"]
        )
        df_samples["Radiograph Yield Tier 1 Count"] = (
            np.where(df_samples["radiograph_tier"].astype(float) < 2, 1, 0)
            * df_samples["Radiograph Count"]
        )

        # ESS mass yield
        df_samples["Mass Count"] = (
            (df_samples["Yield Count"] == 1) & df_samples["US ESS mass"].notna()
        ).astype(int)
        df_samples["Mass Yield Count"] = (
            np.where(df_samples["US ESS mass"].astype(float) >= 410, 1, 0)
            * df_samples["Mass Count"]
        )
        df_samples["Mass Yield Tier 1 Count"] = (
            np.where(df_samples["US ESS mass"].astype(float) >= 450, 1, 0)
            * df_samples["Mass Count"]
        )

        # Combined metrology yield
        df_samples["Metrology Count"] = (
            (df_samples["Yield Count"] == 1)
            & (
                (df_samples["alpha2-tier"].notna())
                | (df_samples["ultrasound_alpha2_tier"].notna())
            )
        ).astype(int)

        df_samples["Tier 1 Count"] = (
            np.where(df_samples["cell_tier"] == 1, 1, 0) * df_samples["Metrology Count"]
        ).astype(int)

        df_samples["Tier 1+2 Count"] = (
            np.where(df_samples["cell_tier"] <= 2, 1, 0) * df_samples["Metrology Count"]
        ).astype(int)

    except:
        df_samples["Anode Count"] = 0
        df_samples["Anode Yield Count"] = 0
        df_samples["Anode Yield Tier 1 Count"] = 0
        df_samples["Ultrasound Count"] = 0
        df_samples["Ultrasound Yield Count"] = 0
        df_samples["Mass Count"] = 0
        df_samples["Mass Yield Count"] = 0
        df_samples["Radiograph Count"] = 0
        df_samples["Radiograph Yield Count"] = 0

    return df_samples
