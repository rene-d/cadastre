# cadastre
Outils pour exploiter le Plan Cadastral Informatisé


## Outils

### fantoir.py


### parcelles.py


#### Description du fichier .yaml

couleur:
* _hexBinary value: aabbggrr_  cf. [\<color>](https://developers.google.com/kml/documentation/kmlreference#colorstyle)
* #rrggbb
* red, blue, green : différentes teintes de la couleur mentionnée
* all : alternance de teintes de rouge, bleu, vert


## Notes

### Création du jeu de test de fantoir.py

    unzip FANTOIR0718.zip
    sed '10,8023000d' FANTOIR0718 > fantoir_demo
    zip fantoir_demo.zip fantoir_demo
