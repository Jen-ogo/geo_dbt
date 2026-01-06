WITH film_revenue AS (
    SELECT 
        i.film_id,
        SUM(p.amount) AS total_revenue
    FROM sakila.payment p
    JOIN sakila.rental r ON p.rental_id = r.rental_id
    JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
    GROUP BY i.film_id
),

rental_count AS (
    SELECT 
        i.film_id,
        COUNT(*) AS total_rentals
    FROM sakila.rental r
    JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
    GROUP BY i.film_id
)

SELECT 
    f.film_id,
    f.title,
    f.rental_rate,
    COALESCE(fr.total_revenue, 0) AS total_revenue,  -- Handling NULL
    COALESCE(rc.total_rentals, 0) AS total_rentals   -- Handling NULL
FROM sakila.film f
LEFT JOIN film_revenue fr ON f.film_id = fr.film_id
LEFT JOIN rental_count rc ON f.film_id = rc.film_id
