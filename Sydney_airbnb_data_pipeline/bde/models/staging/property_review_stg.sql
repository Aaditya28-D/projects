WITH median_values AS (
  SELECT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_rating) AS median_review_scores_rating,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_accuracy) AS median_review_scores_accuracy,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_cleanliness) AS median_review_scores_cleanliness,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_checkin) AS median_review_scores_checkin,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_communication) AS median_review_scores_communication,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_scores_value) AS median_review_scores_value
  FROM {{ ref('airbnb_listing_all_view') }}
)

SELECT
  alv.listing_id,
  alv.number_of_reviews,
  COALESCE(alv.review_scores_rating, mv.median_review_scores_rating) AS review_scores_rating,
  COALESCE(alv.review_scores_accuracy, mv.median_review_scores_accuracy) AS review_scores_accuracy,
  COALESCE(alv.review_scores_cleanliness, mv.median_review_scores_cleanliness) AS review_scores_cleanliness,
  COALESCE(alv.review_scores_checkin, mv.median_review_scores_checkin) AS review_scores_checkin,
  COALESCE(alv.review_scores_communication, mv.median_review_scores_communication) AS review_scores_communication,
  COALESCE(alv.review_scores_value, mv.median_review_scores_value) AS review_scores_value,
  alv.host_id,
  alv.scraped_date
FROM {{ ref('airbnb_listing_all_view') }} alv
CROSS JOIN median_values mv
