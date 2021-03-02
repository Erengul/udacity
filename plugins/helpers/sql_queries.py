class SqlQueries:
    listings_table_insert = ("""
    select listing_id ,
            name ,
            host_id ,
            latitude,
            longitude,
            property_type ,
            room_type ,
            accommodates ,
            bathrooms ,
            bedrooms ,
            amenities,
            price ,
            minimum_nights ,
            maximum_nights ,
            availability_365 ,
            calendar_last_scraped,
            number_of_reviews ,
            last_review_date ,
            review_scores_rating ,
            review_scores_accuracy ,
            review_scores_cleanliness ,
            review_scores_checkin ,
            review_scores_communication	,
            review_scores_location ,
            review_scores_value	,
            reviews_per_month
            from public.staging_listings
    """)

    reviews_table_insert = ("""
        select r.listing_id,
               r.id,
               r."date",
               r.reviewer_id,
               r.comments
        from public.staging_reviews r
        left join public.staging_listings l
	        on r.listing_id = l.listing_id
    """)

    hosts_table_insert = ("""
        SELECT host_id ,
                host_name ,
                host_response_rate ,
                host_is_superhost ,
                host_total_listings_count
        FROM   (SELECT *,
               Row_Number()OVER(partition BY host_id Order by calendar_last_scraped) AS RN
               FROM   public.staging_listings) A
        WHERE  RN = 1 ;
    """)

    locations_table_insert = ("""
        SELECT  latitude,
    			longitude ,
                street,
                city ,
                neighbourhood_cleansed ,
                state ,
                country
        FROM   (SELECT *,
                    Row_Number()OVER(partition BY latitude, longitude Order by calendar_last_scraped) AS RN
               FROM   public.staging_listings) A
        WHERE  RN = 1
    """)

    reviewers_table_insert = ("""
            SELECT 	reviewer_id,
		            reviewer_name
            FROM   (SELECT *,
                    Row_Number()OVER(partition BY reviewer_id Order by date) AS RN
                    FROM   public.staging_reviews) A
            WHERE  RN = 1
    """)

    time_table_insert = ("""
        SELECT date, extract(hour from date), extract(day from date), extract(week from date),
               extract(month from date), extract(year from date), extract(dayofweek from date)
        FROM public.staging_reviews
    """)
