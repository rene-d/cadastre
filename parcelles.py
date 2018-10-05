#! /usr/bin/env python3
# rene-d 2018
# Unlicense: http://unlicense.org

# données GeoJSON bruts: https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/
#
# pages dédiées: https://cadastre.data.gouv.fr/datasets/cadastre-etalab
#                https://cadastre.data.gouv.fr/datasets/plan-cadastral-informatise

# exemples:
#  ./parcelles.py -o elysee -p 75108BO25 -c 75108
#  ./parcelles.py -f demo.yaml

import json
import argparse
import requests
import requests_cache
import datetime
import re
import logging
import gzip
import simplekml
from collections import defaultdict
import yaml
import os


def get_geojson(commune, source="parcelles"):
    """
    charge une donnée du PCI au format GeoJSON
    les sources disponibles sont:
        ['batiments', 'communes', 'feuilles', 'lieux_dits', 'parcelles',
         'prefixes_sections', 'sections', 'subdivisions_fiscales']
    """

    # récupère le numéro de département
    # pour l'Outre-mer, l'arborescence prend bien en compte le numéro à 3 chiffres
    commune = str(commune)
    dept = commune[:2]
    if dept == "97":
        dept = commune[:3]

    # l'url du document GeoJSON à downloader
    url = f'https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/communes/{dept}/{commune}/cadastre-{commune}-{source}.json.gz'

    req = requests.get(url)
    if req.status_code != 200:
        logging.info("url: %s", url)
        logging.error("mauvaise commune ou problème réseau (HTTP {})".format(req.status_code))
    else:
        try:
            return json.loads(gzip.decompress(req.content))
        except Exception as e:
            logging.error("Impossible d'analyser le contenu GeoJSON: {}".format(e))


def add_feature(kml, feature, color, name=None):
    """
    ajoute à un kml un polygone rempli à partir d'une feature GeoJSON
    """

    if not name:
        name = feature['id']

    if feature['geometry']['type'] == 'MultiPolygon':
        pol = kml.newmultigeometry(name=name)
        for seg in feature['geometry']['coordinates'][0]:
            pol.newpolygon(outerboundaryis=seg)
    elif feature['geometry']['type'] == 'Polygon':
        pol = kml.newpolygon(name=name, outerboundaryis=feature['geometry']['coordinates'][0])
    else:
        print('type {} non géré :('.format(feature['geometry']['type']))
        exit(2)

    pol.style.polystyle.color = color
    pol.style.polystyle.outline = 0


def add_feature_contour(kml, feature, color, name=None):
    """
    ajoute à un kml un polygone (non rempli) à partir d'une feature GeoJSON
    """

    if not name:
        name = feature['id']

    if feature['geometry']['type'] == 'MultiPolygon':
        pol = kml.newmultigeometry(name=name)
        for seg in feature['geometry']['coordinates'][0]:
            pol.newpolygon(outerboundaryis=seg)
    elif feature['geometry']['type'] == 'Polygon':
        pol = kml.newpolygon(name=name, outerboundaryis=feature['geometry']['coordinates'][0])
    else:
        print('type {} non géré :('.format(feature['geometry']['type']))
        exit(2)

    pol.style.polystyle.fill = 0
    pol.style.linestyle.color = color


class Parcelles:
    """
    gestion du fichier parcelles du PCI
    """

    def __init__(self, parser, args, commune_courante=None):
        """
        initialise la liste des parcelles
        chaque numéro peut contenir le code commune et le préfixe (qui deviendront les valeurs par défaut)
        obligatoire: section et numéro de plan
        """
        self.parcelles = defaultdict(lambda: [])
        if not args:
            return
        for p1 in args:
            for p2 in p1.upper().split(','):
                m = re.match(r"(\d[\dAB]\d{3})?(\d{0,3})([0A-Z]?[A-Z])(\d{1,4})", p2)
                if not m:
                    parser.error("Mauvais id de parcelle: %s", p2)
                    continue
                commune, prefixe, section, numero = m.groups()
                if not commune:
                    commune = commune_courante
                else:
                    commune_courante = commune
                if not commune_courante:
                    # on ne peut rien faire tant qu'on n'a pas défini de commune
                    parser.error("Id de parcelle sans commune: %s", p2)
                    continue
                if not prefixe:
                    prefixe = '0'
                if len(section) == 1:
                    section = '0' + section
                id = '{}{:03d}{}{:04d}'.format(commune, int(prefixe), section, int(numero))
                self.parcelles[commune].append(id)

    def to_kml(self, kml, color_scheme="all"):
        """
        ajoute le dessin des parcelles à un kml
        color_scheme : méthode pour affecter des couleurs
        """
        ncolor = 0

        if color_scheme == "red":
            colors = ['99{:02x}{:02x}ff'.format(max(0, 64 * i - 1), max(0, 64 * i - 1)) for i in range(3)]
        elif color_scheme == "green":
            colors = ['99{:02x}ff{:02x}'.format(max(0, 64 * i - 1), max(0, 64 * i - 1)) for i in range(3)]
        elif color_scheme == "blue":
            colors = ['99ff{:02x}{:02x}'.format(max(0, 64 * i - 1), max(0, 64 * i - 1)) for i in range(3)]
        else:
            colors = ['990000ff',  # Transparent red
                      '9900ff00',
                      '99ff0000',
                      '997f7fff',
                      '997fff7f',
                      '997f7fff']

        for commune, liste_id in self.parcelles.items():
            data = get_geojson(commune)

            for feature in data['features']:
                properties = feature['properties']
                for id in liste_id:
                    if properties['id'] == id:
                        print("parcelle {id} :  taille {contenance:>10}  créée {created}, mise à jour {updated}".format(**properties))
                        logging.debug(feature['properties'])

                        add_feature(kml, feature, colors[ncolor % len(colors)])
                        ncolor += 1


class Communes:
    """
    gestion du fichier communes du PCI
    """

    def __init__(self, parser, communes):
        """
        initialise la liste des communes (avec le code commune)
        """
        self.communes = set()
        if not communes:
            return
        for commune in communes:
            commune = str(commune)
            if not re.match(r"\d[\dAB]\d{3}", commune, re.I):
                parser.error("Mauvais numéro de commune: %s" % commune)
            else:
                self.communes.add(commune.upper())

    def to_kml(self, kml):
        """
        ajoute le contour des communes sélectionnées au kml, trait vert
        """
        for commune in self.communes:
            data = get_geojson(commune, "communes")

            for feature in data['features']:
                properties = feature['properties']
                print("commune {id} : nom {nom}  créée {created}, mise à jour {updated}".format(**properties))
                add_feature_contour(kml, feature, 'ff2fff2f')


class LieuxDits:
    """
    gestion du fichier lieux_dits du PCI
    """

    def __init__(self, parser, args, commune_courante=None):
        """
        """
        self.parcelles = defaultdict(lambda: [])
        if not args:
            return
        for p1 in args:
            for p2 in p1.upper().split(','):
                m = re.match(r"(\d[\dAB]\d{3}:)?([A-Z.\-'_ ]+|\*)", p2)
                if not m:
                    parser.error("Mauvais id de parcelle: %s" % p2)
                    continue
                commune, lieu_dit = m.groups()
                if not commune:
                    commune = commune_courante
                else:
                    commune_courante = commune[:-1]
                if not commune_courante:
                    # on ne peut rien faire tant qu'on n'a pas défini de commune
                    parser.error("Lieu-dit sans commune: %s", p2)
                    continue
                self.parcelles[commune_courante].append(lieu_dit)

    def to_kml(self, kml):
        """
        ajoute le contour des lieux-dits sélectionnés au kml, trait rouge
        """
        for commune, liste_id in self.parcelles.items():
            data = get_geojson(commune, "lieux_dits")

            for feature in data['features']:
                properties = feature['properties']
                for nom in liste_id:
                    if nom == '*' or properties['nom'].upper() == nom:
                        properties = feature['properties']
                        print("lieu-dit {nom} : commune {commune}  créée {created}, mise à jour {updated}".format(**properties))
                        add_feature_contour(kml, feature, 'ff1522fc', properties['nom'] or '??')


def main():

    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.INFO, datefmt='%H:%M:%S')
    requests_cache.install_cache('cache', allowable_methods=('GET'), expire_after=datetime.timedelta(days=30))

    parser = argparse.ArgumentParser(description='Exporte les parcelles cadastrales en .kml ou .kmz')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-o', '--output', help='fichier à créer', default='export')
    parser.add_argument('-p', '--parcelle', action='append', help="Parcelle ou liste")
    parser.add_argument('-c', '--commune', action='append', help="Commune")
    parser.add_argument('-l', '--lieu-dit', action='append', help="Lieu-dit ou liste")
    parser.add_argument('-f', '--file', action='append', help="Fichier de configuration")
    parser.add_argument('-n', '--dry-run', help="Ne fait rien", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug(args)

    # vérifie l'extension du fichier de sortie
    _, ext = os.path.splitext(args.output)
    if ext != '' and ext != '.kml' and ext != '.kmz':
        parser.error("Extension inconnue: %s" % args.output)

    # création en mémoire du document kml
    kml = simplekml.Kml()

    # ajoute les parcelles, communes, lieux-dits
    parcelles = Parcelles(parser, args.parcelle)
    parcelles.to_kml(kml)

    communes = Communes(parser, args.commune)
    communes.to_kml(kml)

    lieux_dits = LieuxDits(parser, args.lieu_dit)
    lieux_dits.to_kml(kml)

    # si on a donné un ou plusieurs fichiers de configuration
    for file in args.file:
        conf = yaml.load(open(file))
        for kv in conf:
            for k, v in kv.items():
                if k == 'communes':
                    zz = kml.newfolder(name='Communes')
                    c = Communes(parser, v)
                    c.to_kml(zz)

                elif k == 'parcelles':
                    zz = kml.newfolder(name=v.get('titre', 'Parcelles'))
                    c = Parcelles(parser, [','.join(i.split()) for i in v['numero']], v.get('commune'))
                    c.to_kml(zz, v['color_scheme'])

                elif k == 'lieux-dits':
                    zz = kml.newfolder(name='Lieux-Dits')
                    c = LieuxDits(parser, v['nom'], v.get('commune'))
                    c.to_kml(zz)

    # crée le fichier de sortie, en kml ou kmz
    if not args.dry_run:
        if ext == '':
            kml.save(f"{args.output}.kml")
            logging.debug("Fichier créé: %s.kml", args.output)
        elif ext == '.kml':
            kml.save(f"{args.output}")
            logging.debug("Fichier créé: %s", args.output)
        elif ext == '.kmz':
            kml.savekmz(f"{args.output}")
            logging.debug("Fichier créé: %s", args.output)


if __name__ == '__main__':
    main()
