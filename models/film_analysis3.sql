{{ config(materialized='table') }}

WITH film_data AS (
    SELECT film_id, title, rental_rate
    FROM sakila.film
)

SELECT * FROM film_data
