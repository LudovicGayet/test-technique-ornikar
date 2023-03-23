## Question 1

### A

La réponse se trouve dans le fichier `Question 1/question_a.sql`.   
Le fichier est aggrémenté de commentaires qui complètent le code

### B

Dans cette question j'ai supposé que le département associé à un meeting point était pré-calculé en amont et disponible dans la table `meeting_points`.
Pour que le code fonctionne j'associe un meeting point à un departement de manière aléatoire : `SELECT CAST(ROUND(75 + RAND() * (75 - 80)) AS INT64) AS departement`

Cela pour plusieurs raison :
- C'est une donnée stable dans le temps donc probablement qu'on peut se satisfaire de la calculer une seule fois, à l'ingestion et de l'update au besoin
- La nature du besoin implique probablement un notion de fraicheur de donnée quotidienne. Donc on peut imaginer un job qui calcule de departement d'un meeting point avant qu'on execute ce script
- Diminution des performances (Rate-limit de l'API, indisponibilitié, temps de réponse) si calcul à la volée pour chaque requête

Voici quelques pistes sur comment on pourrait extraire le département à partir de la latitude/longitude :
- API publique : `curl https://api-adresse.data.gouv.fr/reverse/?lon=5.7338468&lat=45.1568911`
- Open data

## Question 2

Imagine maintenant que l'on a 100 000 000 de créneaux et qu'on a 1000 nouveaux
créneaux qui arrivent par seconde

### A

Premièrement on peut s'intérogger sur la nature du besoin et la notion de fraicheur de donnée.
On peut supposer que le souhait de récupérer le nombre de leçons par département en fonction du type de partenariat est un besoin "batch" qu'on calcule de manière quotidienne.

Aujourd'hui mon script s'appuie sur les capacités de calcul de Bigquery, donc 100 000 000 de creneaux et 1000 nouveaux par secondes ne pose pas de problème de performances.
En revanche on peut jouer sur les coûts et donc la quantité de donnée lue en clusterisant par "type" et en partitionant par "lesson_start_at".

En revanche mon script ne gère pas la réconciliation d'un meeting point avec un département.
Cette étape peut-être faite lors de la pipeline d'ingestion de donnée qui abouti à la table `meeting_point`

### B

**_On peut exposer ce modèle à l'aide d'une API._**   
Au moment de la création d'un créneau, notre API est appelée par l'application en charge du _cycle de vie d'un creneau_ et renvoie une probabilité. C'est un modèle _scalable_ qui nécessite l'ajout de _workers_ en cas d'augmentation du traffic entrant.

Cela nécessite également qu'en cas **d'insponibilité/problème** de notre API, l'application en charge du _cycle de vie d'un creneau_ doit implémenter un système de _retry asynchrone_.
En cas de **corruption du modèle** il faut aussi voir comment _rejouer_ ces différents créneaux pour prédire à nouveau leur probabilité.
La complexité du workflow se situe côté application en charge du _cycle de vie d'un creneau_.

**_Ou bien construire un produit DATA_**.   
Si on veut _renverser la complexité_ côté team Data, on peut imaginer un système de _queues_ ou la **responsabilité** de l'application en charge du _cycle de vie d'un creneau_ se résume à prévenir l'application Data qu'un nouveau créneau a été créé et de fournir les informations du créneau. L'application data process ensuite de manière asynchrone le message et communique à l'application la probabilité resultante. C'est également une solution _scalable_ car plusieurs workers peuvent dépiler la queue.
En cas **d'indisponibilité** de l'API, l'application _cycle de vie d'un creneau_ n'a rien à faire. En cas de **corruption** du modèle, de même, l'application Data se chargera de renvoyer de nouveaux messages pour les prédictions _corrompues_.
