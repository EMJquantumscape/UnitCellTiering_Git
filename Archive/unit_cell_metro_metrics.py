# %%
import met_client as app
import numpy as np
import pandas as pd
from tqdm import tqdm


def get_anode_metrics(sample, agent, date_filter=np.NaN):
    try:
        sq = app.SearchQuery(
            sample_prefix=sample,
            lregex="HIFISCDS-(TOP|BOTTOM)-PART-SUR-GENERIC_HEATMAP".lower(),
            limit=1000,
        )
        im_scds = agent.search(sq)

        # filter out images taken after date_filter
        if not np.isnan(date_filter):
            im_scds = im_scds[im_scds["acquire_time"] < (date_filter + 12 * 3600)]

        # filter images taken within 1 hour of the first one
        im_scds["acquire_time"] = (
            im_scds["acquire_time"] - im_scds["acquire_time"].min()
        )
        im_scds = im_scds[im_scds["acquire_time"] < 3600]

        # filter the two most recent images (in case there are scans retaken due to measurement error)
        im_scds = im_scds.sort_values("acquire_time", ascending=False).iloc[:2]

        im_scds["defect_total_area"] = im_scds[
            "alpha1-active_circular_defect_total_area"
        ].max()
        im_scds["defect_max_diameter_um"] = im_scds[
            "alpha1-active_circular_defect_max_diameter_um"
        ].max()
        im_scds["defect_median_diameter_um"] = im_scds[
            "alpha1-active_circular_defect_median_diameter_um"
        ].max()
        im_scds["category"] = im_scds["alpha1-hifiscds-category"].max()

        return im_scds[
            [
                "sample",
                "defect_total_area",
                "defect_max_diameter_um",
                "defect_median_diameter_um",
                "category",
            ]
        ].iloc[0]

    except Exception as e:
        print(e)
        if "0" in str(e):
            # try again
            return get_anode_metrics(sample, date_filter, agent)

        return np.NaN, np.NaN, np.NaN


def get_anode_tier(batches, agent):
    # batches = ["APD184..-US"]  # "APD152E.-US", "PMP067..-US", "APD174..-US"]
    results = []
    for batch in tqdm(batches):
        sq = app.SearchQuery(
            sample_prefix=batch,
            lregex="HIFISCDS-(TOP|BOTTOM)-PART-SUR-GENERIC_HEATMAP".lower(),
            limit=10000,
        )
        im_scds = agent.search(sq)

        if (im_scds is None) or (len(im_scds) == 0):
            continue

        # group by sample and remove the ones with an acquire_time difference of more than 1 hour with respect to the first one
        im_scds["acquire_time_diff"] = im_scds["acquire_time"] - im_scds.groupby(
            "sample"
        )["acquire_time"].transform("min")

        # filter scans taken 1 day after initial scan (to avoid reliability scans)
        im_scds = im_scds[im_scds["acquire_time_diff"] < 3600 * 24]

        # group by sample and take the two with the higher acquire_time_diff value (i.e. the two most recent scans in case there was a rescan)
        im_scds = (
            im_scds.sort_values("acquire_time_diff", ascending=False)
            .groupby("sample")
            .head(2)
        )

        # if columns alpha1-hifiscds-category, alpha2_center_tier, alpha2_edge_tier, alpha2_anode_tier are not present, add them
        if "alpha1-hifiscds-category" not in im_scds.columns:
            im_scds["alpha1-hifiscds-category"] = np.NaN
        if "alpha2_center_tier" not in im_scds.columns:
            im_scds["alpha2_center_tier"] = np.NaN
        if "alpha2_edge_tier" not in im_scds.columns:
            im_scds["alpha2_edge_tier"] = np.NaN
        if "alpha2_anode_tier" not in im_scds.columns:
            im_scds["alpha2_anode_tier"] = np.NaN

        im_scds["alpha1-category"] = im_scds.groupby("sample")[
            "alpha1-hifiscds-category"
        ].transform("max")

        im_scds["alpha2-center-tier"] = im_scds.groupby("sample")[
            "alpha2_center_tier"
        ].transform("max")
        im_scds["alpha2-edge-tier"] = im_scds.groupby("sample")[
            "alpha2_edge_tier"
        ].transform("max")
        im_scds["alpha2-tier"] = im_scds.groupby("sample")[
            "alpha2_anode_tier"
        ].transform("max")

        # drop all columns except sample, category and tier
        im_scds = im_scds[
            [
                "sample",
                "alpha1-category",
                "alpha2-center-tier",
                "alpha2-edge-tier",
                "alpha2-tier",
            ]
        ].drop_duplicates()

        results.append(im_scds)

    if len(results) > 0:
        results = pd.concat(results)
    else:
        results = pd.DataFrame(
            columns=[
                "sample",
                "alpha1-category",
                "alpha2-center-tier",
                "alpha2-edge-tier",
                "alpha2-tier",
            ]
        )

    # results.to_clipboard(index=False)

    return results


def get_thickness_metrics(batches, agent):

    results = []
    for batch in tqdm(batches):
        sq = app.SearchQuery(
            sample_prefix=batch,
            a_type=app.constants.AnalysisType.HEATMAP_3D,
            lregex="HIFISCDS-THICKNESS-PART-SUR-REGIONAL_THICKNESS".lower(),
            limit=10000,
        )
        im_thickness = agent.search(sq)

        if (im_thickness is None) or (len(im_thickness) == 0):
            continue

        # group by sample and remove the ones with an acquire_time difference of more than 24 hour with respect to the first one (to avoid reliability scans)
        im_thickness["acquire_time_diff"] = im_thickness[
            "acquire_time"
        ] - im_thickness.groupby("sample")["acquire_time"].transform("min")

        im_thickness = im_thickness[im_thickness["acquire_time_diff"] < 3600 * 24]

        # group by sample and take the one with the higher acquire_time_diff value (i.e. the  most recent scan in case there was a rescan)
        im_thickness = (
            im_thickness.sort_values("acquire_time_diff", ascending=False)
            .groupby("sample")
            .head(1)
        )

        results.append(im_thickness)

    if len(results) > 0:
        results = pd.concat(results)
    else:
        results = pd.DataFrame(
            columns=[
                "sample",
            ]
        )

    return results


def get_ultrasound_tier(batches, agent):
    results = []

    for batch in tqdm(batches):
        sq = app.SearchQuery(
            sample_prefix=batch,
            lregex="ULTRASOUND-GREYSCALE".lower(),
            limit=10000,
        )
        im_ultrasound = agent.search(sq)

        if (im_ultrasound is None) or (len(im_ultrasound) == 0):
            continue

        # group by sample and remove the ones with an acquire_time difference of more than 24 hour with respect to the first one (to avoid reliability scans)
        im_ultrasound["acquire_time_diff"] = im_ultrasound[
            "acquire_time"
        ] - im_ultrasound.groupby("sample")["acquire_time"].transform("min")

        im_ultrasound = im_ultrasound[im_ultrasound["acquire_time_diff"] < 3600 * 24]

        # group by sample and take the one with the higher acquire_time_diff value (i.e. the  most recent scan in case there was a rescan)
        im_ultrasound = (
            im_ultrasound.sort_values("acquire_time_diff", ascending=False)
            .groupby("sample")
            .head(1)
        )

        # if column ultrasound_alpha2_tier is not present, add it
        if "ultrasound_alpha2_tier" not in im_ultrasound.columns:
            im_ultrasound["ultrasound_alpha2_tier"] = np.NaN

        if "defect_total_area" not in im_ultrasound.columns:
            im_ultrasound["defect_total_area"] = np.NaN

        im_ultrasound["ultrasound_alpha2_tier"] = im_ultrasound.groupby("sample")[
            "ultrasound_alpha2_tier"
        ].transform("max")

        # drop all columns except sample, category and tier
        im_ultrasound = im_ultrasound[
            [
                "sample",
                "ultrasound_alpha2_tier",
                "defect_total_area",
            ]
        ].drop_duplicates()

        results.append(im_ultrasound)

    if len(results) > 0:
        results = pd.concat(results)
    else:
        results = pd.DataFrame(
            columns=[
                "sample",
                "ultrasound_alpha2_tier",
                "defect_total_area",
            ]
        )

    # results.to_clipboard(index=False)

    return results


def get_radiograph_tier(batches, agent):
    results = []

    # batches = ["APD193AW-US00", "APD193AW-US01"]
    # agent = app.ImageAgent()

    for batch in tqdm(batches):
        sq = app.SearchQuery(
            sample_prefix=batch,
            lregex="NORDSON_MATRIX-(CS|US|FA)-STITCH-ORIENTED".lower(),
            limit=10000,
        )
        im_radiograph = agent.search(sq)

        if (im_radiograph is None) or (len(im_radiograph) == 0):
            continue

        # group by sample and remove the ones with an acquire_time difference of more than 24 hour with respect to the first one (to avoid reliability scans)
        im_radiograph["acquire_time_diff"] = im_radiograph[
            "acquire_time"
        ] - im_radiograph.groupby("sample")["acquire_time"].transform("min")

        im_radiograph = im_radiograph[im_radiograph["acquire_time_diff"] < 3600 * 24]

        # group by sample and take the one with the higher acquire_time_diff value (i.e. the  most recent scan in case there was a rescan)
        # im_radiograph = (
        #     im_radiograph.sort_values("acquire_time_diff", ascending=False)
        #     .groupby("sample")
        #     .head(1)
        # )

        # if column tier is not present, add it
        if "tier" not in im_radiograph.columns:
            im_radiograph["tier"] = np.NaN

        if "all_contours_catholyte_pct" not in im_radiograph.columns:
            im_radiograph["all_contours_catholyte_pct"] = np.NaN

        if "median_contour_catholyte_pct" not in im_radiograph.columns:
            im_radiograph["median_contour_catholyte_pct"] = np.NaN

        im_radiograph["tier"] = im_radiograph.groupby("sample")["tier"].transform("max")
        # rename tier column to radiograph_tier
        im_radiograph.rename(columns={"tier": "radiograph_tier"}, inplace=True)

        im_radiograph["all_contours_catholyte_pct"] = im_radiograph.groupby("sample")[
            "all_contours_catholyte_pct"
        ].transform("max")

        im_radiograph["median_contour_catholyte_pct"] = im_radiograph.groupby("sample")[
            "median_contour_catholyte_pct"
        ].transform("max")

        # drop all columns except sample, category and tier
        im_radiograph = im_radiograph[
            [
                "sample",
                "radiograph_tier",
                "all_contours_catholyte_pct",
                "median_contour_catholyte_pct",
            ]
        ].drop_duplicates()

        results.append(im_radiograph)

    if len(results) > 0:
        results = pd.concat(results)
    else:
        results = pd.DataFrame(
            columns=[
                "sample",
                "radiograph_tier",
                "all_contours_catholyte_pct",
                "median_contour_catholyte_pct",
            ]
        )

    return results


def get_pupp_metrics(sample, agent, date_filter=np.NaN):
    try:
        sq = app.SearchQuery(
            sample_prefix=sample,
            a_type=app.constants.AnalysisType.HEATMAP_3D,
            # lregex="HIFISCDS-THICKNESS-PART-SUR-HEATMAP".lower(),
            limit=1000,
        )
        im_scds = agent.search(sq)

        # filter out images taken after date_filter
        if not np.isnan(date_filter):
            im_scds = im_scds[im_scds["acquire_time"] < (date_filter + 12 * 3600)]

        # filter images taken within 1 hour of the first one
        im_scds["acquire_time"] = (
            im_scds["acquire_time"] - im_scds["acquire_time"].min()
        )
        im_scds = im_scds[im_scds["acquire_time"] < 3600]

        # filter the two most recent images (in case there are scans retaken due to measurement error)
        im_scds = im_scds.sort_values("acquire_time", ascending=False).iloc[:2]

        return im_scds.iloc[0]

    except Exception as e:
        return None


# %%
