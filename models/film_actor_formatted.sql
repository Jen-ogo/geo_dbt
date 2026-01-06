


SELECT 
  



        f.film_id, 
        CONCAT(
            UPPER(SUBSTRING(f.title, 1, 1)),
            LOWER(SUBSTRING(f.title, 2))
        ) AS formatted_f_title, 
        a.actor_id, 
        CONCAT(
            UPPER(SUBSTRING(a.first_name, 1, 1)),
            LOWER(SUBSTRING(a.first_name, 2))
        ) AS formatted_a_first_name, 
        a.last_name

FROM sakila.film f
JOIN sakila.film_actor fa ON f.film_id = fa.film_id
JOIN sakila.actor a ON fa.actor_id = a.actor_id