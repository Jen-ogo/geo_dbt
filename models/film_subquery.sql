SELECT 
    f.film_id,
    f.title,
    f.rental_rate,

    -- Subquery for total revenue per film with NULL handling
    COALESCE((
        SELECT SUM(p.amount)
        FROM sakila.payment p
        JOIN sakila.rental r ON p.rental_id = r.rental_id
        JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
        WHERE i.film_id = f.film_id
    ), 0) AS total_revenue,

    -- Subquery for total rentals per film with NULL handling
    COALESCE((
        SELECT COUNT(*)
        FROM sakila.rental r
        JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
        WHERE i.film_id = f.film_id
    ), 0) AS total_rentals

FROM sakila.film f
