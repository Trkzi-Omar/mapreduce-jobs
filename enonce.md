Here is the content converted into Markdown format:

```markdown
# Développement Web Python - Big Data: Batch Processing 2024

**Mr. Abdelmajid BOUSSELHAM**

---

## TP 1: Map Reduce

### Exercice 1

1. Développez un Job Map Reduce permettant, à partir d’un fichier texte (`ventes.txt`) en entrée, contenant les ventes d’une entreprise dans différentes villes, de déterminer le **total des ventes par ville**.  
   - Structure du fichier `ventes.txt` :
     ```
     date ville produit prix
     ```
   - Testez votre code en lançant le job sur un cluster Hadoop.

2. Créez un deuxième job permettant de **calculer le prix total des ventes des produits par ville pour une année donnée**.

---

### Exercice 2

On dispose d’un grand ensemble de fichiers journaux (logs) Web générés par un serveur. Chaque ligne du fichier journal contient des informations sur les requêtes HTTP, y compris :
- L'adresse IP du client
- La date
- Le chemin d'accès demandé
- Le code de réponse HTTP

#### Objectif :
Analyser les logs pour :
1. Trouver le **nombre total de requêtes par adresse IP**
2. Trouver le **nombre de requêtes réussies (code de réponse HTTP 200) par adresse IP**

#### Données d'entrée :
Un ensemble de fichiers journaux Web au format texte. Exemple :
```
192.168.1.1 - - [12/May/2023:15:30:45 +0000] "GET /page1 HTTP/1.1" 200 1234
192.168.1.2 - - [12/May/2023:15:31:02 +0000] "GET /page2 HTTP/1.1" 404 567
192.168.1.1 - - [12/May/2023:15:32:10 +0000] "GET /page1 HTTP/1.1" 200 789
192.168.1.3 - - [12/May/2023:15:32:35 +0000] "GET /page3 HTTP/1.1" 200 987
```

#### Travail à faire :
Utilisez **Hadoop MapReduce** pour :
- Calculer le **nombre total de requêtes** par adresse IP.
- Calculer le **nombre de requêtes réussies** (code de réponse 200) par adresse IP.
```


