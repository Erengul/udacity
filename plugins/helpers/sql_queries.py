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
        select distinct host_id ,
                host_name ,
                host_response_rate ,
                host_is_superhost ,
                host_total_listings_count 
        from public.staging_listings
    """)

    locations_table_insert = ("""
        select distinct latitude,longitude ,
            street,
            city ,
            neighbourhood_cleansed ,
            state ,
            country	
        from public.staging_listings
    """)
   
    reviewers_table_insert = ("""
        select distinct reviewer_id,
		        reviewer_name
        from public.staging_reviews
    """)
    
    time_table_insert = ("""
        SELECT date, extract(hour from date), extract(day from date), extract(week from date), 
               extract(month from date), extract(year from date), extract(dayofweek from date)
        FROM public.staging_reviews;
    """)