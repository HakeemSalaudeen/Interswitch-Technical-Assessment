WITH WeekendTrips AS (
    -- Calculate daily trip counts for Sat/Sun
    select DATE_TRUNC('day', tpep_pickup_datetime) AS trip_day,
        DATE_TRUNC('month', tpep_pickup_datetime) AS trip_month,
        EXTRACT(DOW FROM tpep_pickup_datetime) AS dow,
        COUNT(*) AS daily_trip_count
    from tripdata
    where tpep_pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31' AND EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6)
    GROUP by trip_day, trip_month, dow),
MonthlyAverages AS (
    -- Calculate average daily trip count per month
    SELECT
        trip_month AS month,
        AVG(CASE WHEN dow = 6 THEN daily_trip_count END) AS sat_mean_trip_count,
        AVG(CASE WHEN dow = 0 THEN daily_trip_count END) AS sun_mean_trip_count
    from WeekendTrips GROUP by trip_month),
MonthlyFareDuration AS (
    -- Calculate average fare and duration per trip per month
    SELECT
        DATE_TRUNC('month', tpep_pickup_datetime) AS month,
        AVG(CASE WHEN EXTRACT(DOW FROM tpep_pickup_datetime) = 6 THEN fare_amount END) AS sat_mean_fare_per_trip,
        AVG(CASE WHEN EXTRACT(DOW FROM tpep_pickup_datetime) = 6 THEN trip_duration END) AS sat_mean_duration_per_trip,
        AVG(CASE WHEN EXTRACT(DOW FROM tpep_pickup_datetime) = 0 THEN fare_amount END) AS sun_mean_fare_per_trip,
        AVG(CASE WHEN EXTRACT(DOW FROM tpep_pickup_datetime) = 0 THEN trip_duration END) AS sun_mean_duration_per_trip
    from tripdata
    where tpep_pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31' AND EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6)
    GROUP by month)
-- Combine results from both CTEs
SELECT
    TO_CHAR(MFD.month, 'YYYY-MM') AS month,
    ROUND(MA.sat_mean_trip_count, 2) AS sat_mean_trip_count,
    ROUND(MFD.sat_mean_fare_per_trip, 2) AS sat_mean_fare_per_trip,
    ROUND(MFD.sat_mean_duration_per_trip, 2) AS sat_mean_duration_per_trip,
    ROUND(MA.sun_mean_trip_count, 2) AS sun_mean_trip_count,
    ROUND(MFD.sun_mean_fare_per_trip, 2) AS sun_mean_fare_per_trip,
    ROUND(MFD.sun_mean_duration_per_trip, 2) AS sun_mean_duration_per_trip
from MonthlyAverages MA join MonthlyFareDuration MFD ON MA.month = MFD.month
order by MFD.month;