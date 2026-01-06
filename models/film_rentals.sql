SELECT 
    i.film_id,
    COUNT(*) AS total_rentals
FROM sakila.rental r
JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
GROUP BY i.film_id
