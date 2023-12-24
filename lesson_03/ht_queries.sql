/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT 
  category.name, 
  COUNT(film_category.film_id) AS film_count 
FROM 
  category 
  JOIN film_category ON category.category_id = film_category.category_id 
GROUP BY 
  category.name 
ORDER BY 
  film_count DESC;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
SELECT 
  actor.first_name, 
  actor.last_name, 
  COUNT(rental.rental_id) AS rental_count 
FROM 
  actor 
  JOIN film_actor ON actor.actor_id = film_actor.actor_id 
  JOIN inventory ON film_actor.film_id = inventory.film_id 
  JOIN rental ON inventory.inventory_id = rental.inventory_id 
GROUP BY 
  actor.first_name, 
  actor.last_name 
ORDER BY 
  rental_count DESC 
LIMIT 
  10;


/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
SELECT 
  category.name, 
  SUM(payment.amount) AS payments_cost, 
  COUNT(payment.amount) AS payments_count 
FROM 
  category 
  JOIN film_category ON category.category_id = film_category.category_id 
  JOIN inventory ON film_category.film_id = inventory.film_id 
  JOIN rental ON inventory.inventory_id = rental.inventory_id 
  JOIN payment ON rental.rental_id = payment.rental_id 
GROUP BY 
  category.name 
ORDER BY 
  payments_cost DESC 
LIMIT 
  1;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT 
  film.title 
FROM 
  film 
  LEFT JOIN inventory ON film.film_id = inventory.film_id 
WHERE 
  inventory.film_id IS NULL;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT 
  actor.first_name, 
  actor.last_name, 
  COUNT(film_actor.film_id) AS film_count 
FROM 
  actor 
  JOIN film_actor ON actor.actor_id = film_actor.actor_id 
  JOIN film_category ON film_actor.film_id = film_category.film_id 
  JOIN category ON film_category.category_id = category.category_id 
WHERE 
  category.name = 'Children' 
GROUP BY 
  actor.first_name, 
  actor.last_name 
ORDER BY 
  film_count DESC 
LIMIT 
  3;
