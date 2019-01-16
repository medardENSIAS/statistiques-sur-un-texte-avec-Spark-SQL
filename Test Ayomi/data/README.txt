 //-----------//-----||-----\\----------\\
//                                       \\
|| T E S T   D A T A   S C I E N T I S T ||
\\                                       //
 \\-----------\\-----||-----//----------//

# Input :

	- Fichier .txt des instructions et consignes

	- log activité sur le site : log.csv
		+ données navigations avec : | typeAction | cookie | idUser | etc | ... |
	- DataBase des users du site : DataBase.csv
		+ base de donnée avec      : | idUser   | mail   | etc    | ... | ... |


# Exercice :

Faire un script (VBA / Js / Python) qui :
	- Importe les infos de log.csv
	- Regroupe les données de chaque utilisateur
		+ en croisant les cookie / idUser
			tips : 	x un cookie est un "id" du navigateur de l'utilisateur du site
					x lorsque l'utilisateur est connecté sur son navigateur : l'idUser est renseigné
		+ en définissant un tableau d'action propre à chaque utilisateur


	- Comptabilise le nombre d'action de chaque utilisateur
	- Définit un segment par user en fonction de son type d'action, défini dans le ficher valueAction.txt
	- Associe l'utilisateur à son mail
	- Compte le nombre d'action réalisé pour chaque compte (mail)

# Output : 

	- Fichier .csv des combinaisons : mail / nbAction / Segment
		+ Feuille 1 : | mail | nbAction | Segment |
	- Fichier .txt expliquant rapidement l'algo
	- Fichier contenant le code
