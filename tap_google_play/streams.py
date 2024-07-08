import pycountry
import backoff

from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th 

from tap_google_play.client import GooglePlayStream

from pendulum import parse

from google_play_scraper import reviews, Sort


@backoff.on_exception(backoff.expo, Exception, max_time=120, max_tries=10)
def retriable_reviews(app_id, lang, country, sort, count, continuation_token):
    return reviews(
        app_id,
        lang = lang,
        country = country,
        sort = Sort.NEWEST,
        count = 1000,
        continuation_token=continuation_token
    )

class ReviewsStream(GooglePlayStream):
    """Define custom stream."""
    name = "reviews"
    primary_keys = ["reviewId"]
    replication_key = "at"
    schema = th.PropertiesList(
        th.Property("userName", th.StringType),
        th.Property("userImage",th.StringType),
        th.Property("content",th.StringType),
        th.Property("score",th.IntegerType),
        th.Property("thumbsUpCount",th.IntegerType),
        th.Property("reviewCreatedVersion",th.StringType),
        th.Property("at",th.DateTimeType),
        th.Property("replyContent",th.StringType),
        th.Property("repliedAt",th.DateTimeType),
        th.Property("reviewId",th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:

        app_id = self.config.get("app_id")

        continuation_token = None
        results = []
        result = True

        start_date = self.get_starting_replication_key_value(context)
        if start_date: 
            start_date =  parse(start_date)

        self.logger.info(f"Getting reviews for {app_id}.")


        countries = self.config.get(
            "countries",
            [getattr(country, 'alpha_2').lower() for country in pycountry.countries]
        )
    
        languages = self.config.get(
            "languages", 
            [getattr(lang, 'alpha_2', None).lower() for lang in pycountry.languages if getattr(lang, 'alpha_2', None) is not None]
        )

        results_dict = {}

        for lang in languages:
            for country in countries:
                self.logger.info(f"Getting reviews for {app_id} written in {lang}")
                continuation_token = None
                result = True
                while result:
                    result, continuation_token = retriable_reviews(
                        app_id,
                        lang = lang,
                        country = country,
                        sort = Sort.NEWEST,
                        count = 1000,
                        continuation_token=continuation_token
                    )
                    for record in result:
                        if record["reviewId"] not in results_dict.keys():
                            results_dict[record["reviewId"]] = record
                    
                    self.logger.info(f"{results_dict.values().__len__()} imported records so far.")
                

                if start_date:
                    filtered_results = []
                    for record in results_dict.values():
                        if record.get('at') > start_date.replace(tzinfo=None): 
                            filtered_results.append(record)
                        
                    return filtered_results


        return results_dict.values()
