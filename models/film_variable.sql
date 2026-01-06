SELECT 
    film_id,
    title,
    rental_rate
FROM sakila.film
WHERE rental_rate = {{ var('rental_rate', 6.99) }}
