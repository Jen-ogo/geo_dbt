SELECT 
    f.film_id,
    f.title,
    f.rental_rate,
    COALESCE(fr.total_revenue, 0) AS total_revenue,
    COALESCE(frental.total_rentals, 0) AS total_rentals
FROM sakila.film f
LEFT JOIN {{ ref('film_revenue') }} fr ON f.film_id = fr.film_id
LEFT JOIN {{ ref('film_rentals') }} frental ON f.film_id = frental.film_id
