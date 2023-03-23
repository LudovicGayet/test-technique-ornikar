-- Pour les 5 enseignants qui ont donné le plus de leçons sur Q3 2020, donnez,
-- pour chacun d’entre eux, la date à laquelle ils ont effectué leur 50ème leçon ?

-- Pour répondre à la question, j'ai procédé en 3 parties :
--      - définition du concept de "leçon effectuée" et extraction des données relatives
--      - extraction du top 5 enseignants sur Q3 2020
--      - calcul de la date de leur 50ème leçon

-- LINTER SQLFLUFF

WITH extract_lecon_effectuees AS (
    SELECT lessons.*
    FROM test_dataset.lessons AS lessons
    -- pas de left join pour ignorer toute leçon sans booking
    INNER JOIN test_dataset.bookings AS bookings
        USING(lesson_id)
    WHERE
        -- La leçon ne doit pas avoir été supprimée, ou bien après que la leçon fût terminé et on considère donc que la leçon a eut lieu malgré la suppression de la leçon
        (
            lessons.lesson_deleted_at IS NULL
            OR (lessons.lesson_start_at < lessons.lesson_deleted_at)
        )
        -- Le Booking ne doit pas avoir été supprimé, ou bien après que la leçon fût terminé et on considère que la leçon a eut lieu malgré la suppression du booking
        AND (
            bookings.booking_deleted_at IS NULL
            OR (bookings.booking_deleted_at > lessons.lesson_start_at)
        )
        -- Le Booking doit avoir été créé avant que l'heure de la leçon début
        AND (bookings.booking_created_at <= lessons.lesson_start_at)
),

extract_top_5_enseignants_sur_q3_2020 AS (
    SELECT
        instructor_id,
        count(*)
    FROM extract_lecon_effectuees
    WHERE date(lesson_start_at) BETWEEN "2020-07-01" AND "2020-09-30"
    GROUP BY instructor_id
    ORDER BY count(*) DESC
    LIMIT 5
),

calcul_date_de_la_50_ieme_lecon AS (
    SELECT
        instructor_id,
        nth_value(
            lesson_id, 50
        ) OVER (
            PARTITION BY instructor_id
            ORDER BY
                lesson_start_at ASC
            RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS cinquante_ieme_lesson_id,
        date(lesson_start_at) AS cinquante_ieme_lesson_date
    FROM extract_top_5_enseignants_sur_q3_2020
    INNER JOIN extract_lecon_effectuees
        USING(instructor_id)
    WHERE date(lesson_start_at) BETWEEN "2020-07-01" AND "2020-09-30"
    -- On garde uniquement la ligne correspondant à la 50ème valeur
    QUALIFY cinquante_ieme_lesson_id = lesson_id
    ORDER BY lesson_start_at ASC
)

SELECT
    instructor_id,
    cinquante_ieme_lesson_id,
    cinquante_ieme_lesson_date
FROM calcul_date_de_la_50_ieme_lecon
