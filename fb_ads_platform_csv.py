### A very simple "job" that creates a table with each ad, and lookups for account, campaign, and adset ###

import os
import csv
import tempfile
import civis
import logging
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights

logging.basicConfig()
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
LOG.info("starting process")

fb_access_token = os.environ.get("FB_ACCESS_TOKEN")
fb_ad_account_id = os.environ.get("FB_AD_ACCOUNT_ID")
database_name = os.environ.get("DATABASE_NAME")
schema_name = os.environ.get("SCHEMA_NAME")
table_name_prefix = os.environ.get("TABLE_NAME_PREFIX")

LOG.info("imported enrivonment vars")
LOG.info("initializing FB Ads API")

FacebookAdsApi.init(access_token=fb_access_token)

LOG.info("initialized FB Ads API")

# See https://developers.facebook.com/docs/marketing-api/reference/ad-account
account_dims = ['account_id', 'name', 'account_status', 'age', 'created_time', 'currency', 'timezone_name']
account_col_names = account_dims.copy()

# See https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group
campaign_dims = ['id', 'account_id', 'name', 'objective', 'start_time',  'stop_time', 'status', 'bid_strategy', 'budget_remaining', 'buying_type', 'daily_budget', 'source_campaign_id', 'spend_cap', 'created_time', 'updated_time']

# See https://developers.facebook.com/docs/marketing-api/reference/ad-campaign
adset_dims = ['id', 'campaign_id', 'account_id', 'name', 'status', 'effective_status', 'start_time', 'end_time', 'bid_amount', 'bid_strategy', 'budget_remaining', 'daily_budget', 'daily_min_spend_target', 'daily_spend_cap', 'lifetime_budget', 'lifetime_imps', 'lifetime_min_spend_target', 'lifetime_spend_cap', 'destination_type', 'is_dynamic_creative', 'source_adset_id',  'created_time', 'updated_time']

# See https://developers.facebook.com/docs/marketing-api/reference/adgroup
ad_dims = ['id', 'adset_id', 'campaign_id', 'account_id', 'name', 'status', 'effective_status', 'bid_amount', 'created_time', 'updated_time']

# See https://developers.facebook.com/docs/marketing-api/reference/adgroup/insights/v5.0
ad_metrics = ['ad_id', 'adset_id', 'campaign_id', 'account_id', 'buying_type', 'spend', 'impressions', 'reach', 'frequency', 'clicks', 'unique_clicks', 'conversions', 'conversion_rate_ranking', 'actions', 'action_values', 'cost_per_action_type', 'ad_click_actions', 'ad_impression_actions', 'age_targeting', 'auction_bid', 'auction_competitiveness', 'auction_max_competitor_bid', 'canvas_avg_view_percent', 'canvas_avg_view_time', 'conversion_values', 'cost_per_15_sec_video_view', 'cost_per_2_sec_continuous_video_view', 'cost_per_ad_click', 'cost_per_conversion', 'cost_per_dda_countby_convs', 'cost_per_estimated_ad_recallers', 'cost_per_inline_link_click', 'cost_per_inline_post_engagement', 'cost_per_one_thousand_ad_impression', 'cost_per_outbound_click', 'cost_per_thruplay', 'cost_per_unique_action_type', 'cost_per_unique_click', 'cost_per_unique_inline_link_click', 'cost_per_unique_outbound_click', 'cpc', 'cpm', 'cpp', 'created_time', 'ctr', 'date_start', 'date_stop', 'dda_countby_convs', 'engagement_rate_ranking', 'estimated_ad_recall_rate', 'estimated_ad_recall_rate_lower_bound', 'estimated_ad_recall_rate_upper_bound', 'estimated_ad_recallers', 'estimated_ad_recallers_lower_bound', 'estimated_ad_recallers_upper_bound', 'full_view_impressions', 'full_view_reach', 'gender_targeting', 'inline_link_click_ctr', 'inline_link_clicks', 'inline_post_engagement', 'instant_experience_clicks_to_open', 'instant_experience_clicks_to_start', 'instant_experience_outbound_clicks', 'labels', 'location', 'mobile_app_purchase_roas', 'objective', 'outbound_clicks', 'outbound_clicks_ctr', 'place_page_name', 'purchase_roas', 'quality_ranking', 'social_spend', 'unique_actions', 'unique_ctr', 'unique_inline_link_click_ctr', 'unique_inline_link_clicks', 'unique_link_clicks_ctr', 'unique_outbound_clicks', 'unique_outbound_clicks_ctr', 'unique_video_view_15_sec', 'updated_time', 'video_15_sec_watched_actions', 'video_30_sec_watched_actions', 'video_avg_time_watched_actions', 'video_continuous_2_sec_watched_actions', 'video_p100_watched_actions', 'video_p25_watched_actions', 'video_p50_watched_actions', 'video_p75_watched_actions', 'video_p95_watched_actions', 'video_play_actions', 'video_play_curve_actions', 'video_play_retention_0_to_15s_actions', 'video_play_retention_20_to_60s_actions', 'video_play_retention_graph_actions', 'video_thruplay_watched_actions', 'video_time_watched_actions', 'website_ctr', 'website_purchase_roas', 'wish_bid']


# Stand-in for importing this from hippo.
def process_civis_jobs(jobs, success_action=None, failure_action=None):
    if not isinstance(jobs, (list, tuple, set)):
        jobs = [jobs]

    num_failed = 0
    for job in jobs:
        try:
            job.result()
            if success_action:
                success_action(job.result())
        except civis.base.CivisJobFailure as err:
            response = err.response
            if failure_action:
                failure_action(response)
            else:
                error = ("Job failure: your job with ID '%s' has failed with "
                         "error message %s")
                if 'exception' in response:
                    message = response['exception']
                elif 'error' in response:
                    message = response['error']
                else:
                    message = ''
                LOG.error(error, str(response['id']), message)
            num_failed += 1

    if num_failed > 0:
        raise civis.base.CivisJobFailure('{} of your {} jobs failed'.format(num_failed, len(jobs)))



# Hit API for account, campaign, etc. and upload to DB.
LOG.info("account")
account = AdAccount(fb_ad_account_id)
with tempfile.NamedTemporaryFile() as account_csv:
    with open(account_csv.name, mode="w", newline="\n") as file_obj:
        account_writer = csv.DictWriter(file_obj, account_dims, quoting=csv.QUOTE_ALL,
                                     restval=None, extrasaction="ignore")
        account_writer.writeheader()
        account_row = account.api_get(fields=account_dims)
        account_writer.writerow(account_row)
    jobs = []
    jobs.append(civis.io.csv_to_civis(filename=account_csv.name,
                                      database=database_name,
                                      table=('{}.{}_accounts'.format(schema_name, table_name_prefix)),
                                      headers=True,
                                      existing_table_rows='drop'))
    process_civis_jobs(jobs)

LOG.info("campaigns")
account = AdAccount(fb_ad_account_id)
campaigns = account.get_campaigns()
with tempfile.NamedTemporaryFile() as campaign_csv:
    with open(campaign_csv.name, mode="w", newline="\n") as file_obj:
        campaign_writer = csv.DictWriter(file_obj, campaign_dims, quoting=csv.QUOTE_ALL, restval=None, extrasaction='ignore')
        campaign_writer.writeheader()
        for campaign in campaigns:
            campaign_row = campaign.api_get(fields=campaign_dims)
            campaign_writer.writerow(campaign_row)
    jobs = []
    jobs.append(civis.io.csv_to_civis(filename=campaign_csv.name,
                                  database=database_name,
                                  table=('{}.{}_campaigns'.format(schema_name, table_name_prefix)),
                                  headers=True,
                                  existing_table_rows='drop'))
    process_civis_jobs(jobs)

LOG.info("adsets")
account = AdAccount(fb_ad_account_id)
adsets = account.get_ad_sets()
with tempfile.NamedTemporaryFile() as adset_csv:
    with open(adset_csv.name, mode="w", newline="\n") as file_obj:
        adset_writer = csv.DictWriter(file_obj, adset_dims, quoting=csv.QUOTE_ALL, restval=None, extrasaction='ignore')
        adset_writer.writeheader()
        for adset in adsets:
            adset_row = adset.api_get(fields=adset_dims)
            adset_writer.writerow(adset_row)
    jobs = []
    jobs.append(civis.io.csv_to_civis(filename=adset_csv.name,
                                  database=database_name,
                                  table=('{}.{}_adsets'.format(schema_name, table_name_prefix)),
                                  headers=True,
                                  existing_table_rows='drop'))
    process_civis_jobs(jobs)

LOG.info("ads")
account = AdAccount(fb_ad_account_id)
ads = account.get_ads()
with tempfile.NamedTemporaryFile() as ad_csv:
    with open(ad_csv.name, mode="w", newline="\n") as file_obj:
        ad_writer = csv.DictWriter(file_obj, ad_dims, quoting=csv.QUOTE_ALL, restval=None, extrasaction='ignore')
        ad_writer.writeheader()
        for ad in ads:
            ad_row = ad.api_get(fields=ad_dims)
            ad_writer.writerow(ad_row)
    jobs = []
    jobs.append(civis.io.csv_to_civis(filename=ad_csv.name,
                                  database=database_name,
                                  table=('{}.{}_ads'.format(schema_name, table_name_prefix)),
                                  headers=True,
                                  existing_table_rows='drop'))
    process_civis_jobs(jobs)

LOG.info("ad_insights")
account = AdAccount(fb_ad_account_id)
ads = account.get_ads()
with tempfile.NamedTemporaryFile() as ad_insight_csv:
    with open(ad_insight_csv.name, mode="w", newline="\n") as file_obj:
        ad_insight_writer = csv.DictWriter(file_obj, ad_metrics, quoting=csv.QUOTE_ALL, restval=None, extrasaction='ignore')
        ad_insight_writer.writeheader()
        for ad in ads:
            # This can easily return multiple rows, for different dates, breakdowns
            ad_insight_rows = ad.get_insights(fields=ad_metrics)
            for row in ad_insight_rows:
                ad_insight_writer.writerow(row)
    jobs = []
    jobs.append(civis.io.csv_to_civis(filename=ad_insight_csv.name,
                                  database=database_name,
                                  table=('{}.{}_ad_insights'.format(schema_name, table_name_prefix)),
                                  headers=True,
                                  existing_table_rows='drop'))
    process_civis_jobs(jobs)
