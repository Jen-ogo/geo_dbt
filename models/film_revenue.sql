SELECT 
    i.film_id,
    SUM(p.amount) AS total_revenue
FROM sakila.payment p
JOIN sakila.rental r ON p.rental_id = r.rental_id
JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
GROUP BY i.film_id
