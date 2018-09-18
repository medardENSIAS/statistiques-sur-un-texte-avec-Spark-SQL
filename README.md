# statistiques-sur-un-texte-avec-Spark-SQL

Cette partie est la première partie de projet de reconnaissance  de sentiments sur Tweeter 

réaliser des statistiques sur la fréquence d'utilisation des mots dans un texte

Vous allez créer une application Spark dans laquelle un DataFrame vous permettra de sélectionner les mots par longueur du mot et par fréquence d'utilisation. Vous devrez réaliser un programme vous permettant de charger un texte passé en argument. Un DataFrame sera créé à partir des mots de ce texte.

Voici les règles d'extraction des lignes et des mots du texte :

    On considère que les mots sont séparés par des espaces.
    Les caractères ,.;:?!"-' en début et en fin de mot seront supprimés.
    Les mots contenant les caractères "@" ou "/" seront supprimés.
    Les mots doivent être ramenés en caractères minuscules.

En suivant ces régles, vous devrez afficher :

    le mot le plus long du texte
    le mot de quatre lettres le plus fréquent
    le mot de quinze lettres le plus fréquent
 -------------------------------------------------------------- Fin de la premiere partie-------------------------------------------

