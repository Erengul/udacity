CREATE TABLE IF NOT EXISTS public.staging_listings(
    listing_id varchar(256),
    name varchar(256),
    host_id varchar(256),
    host_name varchar(256),
    host_response_rate decimal(8,4),
    host_is_superhost varchar(256),
    host_total_listings_count int8,
    street varchar(256),
    city varchar(256),
    neighbourhood_cleansed varchar(256),
    state varchar(256),
    country	varchar(256),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    property_type varchar(256),
    room_type varchar(256),
    accommodates int8,
    bathrooms decimal(8,4),
    bedrooms decimal(8,4),
    amenities varchar(max),
    price numeric(18,0),
    minimum_nights int8,
    maximum_nights int8,
    availability_365 int8,
    calendar_last_scraped timestamp,
    number_of_reviews int8,
    last_review_date timestamp,
    review_scores_rating int8,
    review_scores_accuracy int8,
    review_scores_cleanliness int8,
    review_scores_checkin int8,
    review_scores_communication	int8,
    review_scores_location int8,
    review_scores_value	int8,
    reviews_per_month numeric(18,0)
);


CREATE TABLE IF NOT EXISTS public.staging_reviews(
    listing_id varchar(256),
    id varchar(256),
    "date" timestamp,
    reviewer_id varchar(256),
    reviewer_name varchar(256),
    comments varchar(max)
);


CREATE TABLE IF NOT EXISTS public.listing(
    listing_id varchar(256),
    name varchar(256),
    host_id varchar(256),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    property_type varchar(256),
    room_type varchar(256),
    accommodates int8,
    bathrooms decimal(8,4),
    bedrooms decimal(8,4),
    amenities varchar(max),
    price numeric(18,0),
    minimum_nights int8,
    maximum_nights int8,
    availability_365 int8,
    calendar_last_scraped timestamp,
    number_of_reviews int8,
    last_review_date timestamp,
    review_scores_rating int8,
    review_scores_accuracy int8,
    review_scores_cleanliness int8,
    review_scores_checkin int8,
    review_scores_communication	int8,
    review_scores_location int8,
    review_scores_value	int8,
    reviews_per_month numeric(18,0)
);

    CREATE TABLE IF NOT EXISTS public.hosts(
        host_id varchar(256),
        host_name varchar(256),
        host_response_rate decimal(8,4),
        host_is_superhost varchar(256),
        host_total_listings_count int8
    );


CREATE TABLE IF NOT EXISTS public.locations(
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    street varchar(256),
    city varchar(256),
    neighbourhood_cleansed varchar(256),
    state varchar(256),
    country	varchar(256)
);

CREATE TABLE IF NOT EXISTS public.reviews(
    listing_id varchar(256),
    id varchar(256),
    "date" timestamp,
    reviewer_id varchar(256),
    comments varchar(max)
);

CREATE TABLE IF NOT EXISTS public.reviewers(
    reviewer_id varchar(256),
    reviewer_name varchar(256)
);


CREATE TABLE IF NOT EXISTS public.time(
    "date" timestamp,
    "hour" int4,
    "day" int4,
    "week" int4,
    "month" int4,
    "year" int4,
    dayofweek int4
);