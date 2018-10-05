#! /usr/bin/env python3
# rene-d 2018
# Unlicense: http://unlicense.org

"""
Fichier FANTOIR (Fichier ANnuaire TOpographique Initialisé Réduit)
https://www.data.gouv.fr/fr/datasets/fichier-fantoir-des-voies-et-lieux-dits/

crée une base SQLite3 à partir du fichier texte

lecture d'après la description officielle (cf. site) + ajustements
"""

import zipfile
import sqlite3
import os
import argparse
import glob

try:
    from tqdm import tqdm
except ImportError:
    print("Pour afficher la progression: pip3 install tqdm")

    def tqdm(it, *args, **kwargs):
        return it


#
# description du fichier FANTOIR
#

# le fichier fantoir est une suite de lignes texte, avec un enregistement par ligne
#   initial
#     direction
#       commune
#          voie
#          voie
#          ...
#       commune
#          voie
#          ...
#     ...
#   final

FILLER = '_ '
FILLER_NULL = '_\0'
FILLER_ANY = '_*'
FILLER_0 = '_0'
FILLER_9 = '_9'

enregistrement_initial = [
    "initial",
    [10, FILLER_NULL],
    [ 1, FILLER],
    [25, 'lbl_prod', 'Libellé du centre de production du fichier'],
    [ 8, 'date_situ', 'Date de situation du fichier', 'AAAAMMJJ'],
    [ 7, 'date_prod', 'Date de production du fichier', 'AAAAQQQ']
]

enregistrement_direction = [
    "direction",
    [ 2, 'code_dpt', 'Code département'],
    [ 1, 'code_dir', 'Code direction'],
    [ 8, FILLER],
    [30, 'libelle_dir', 'Libellé Direction']
]

enregistrement_commune = [
    "commune",
    [ 2, 'code_dpt', 'Code département'],
    [ 1, 'code_dir', 'Code direction'],
    [ 3, 'code_comm', 'Code commune'],
    [ 4, FILLER],
    [ 1, 'cle_rivoli', 'Clé RIVOLI', 'Contrôle la validité de COMM'],
    [30, 'libelle_comm', 'Libellé Commune'],
    [ 1, FILLER],
    [ 1, 'type_comm', 'Type de la commune', 'N : rurale, R : recensée'],
    [ 2, FILLER],
    [ 1, 'caract_rur', 'Caractère RUR', '3 : pseudo-recensée, blanc sinon'],
    [ 3, FILLER],
    [ 1, 'caract_popul', 'Caractère de population', 'blanc si < 3000 hab, * sinon'],
    [ 2, FILLER],
    [ 7, 'popul_reel', 'Population réelle'],
    [ 7, 'popul_apart', 'Population à part'],
    [ 7, 'popul_fict', 'Population fictive'],
    [ 1, 'caract_annul', 'Caractère d’annulation', 'Q : annulation avec transfert'],
    [ 7, 'date_annul', 'Date d’annulation'],
    [ 7, 'date_crea', 'Date de création de l’article'],
]

enregistrement_voie = [
    "voie",
    [ 2, 'code_dpt', 'Code département'],
    [ 1, 'code_dir', 'Code direction'],
    [ 3, 'code_comm', 'Code commune'],
    [ 4, 'ident_voie', 'Identifiant de la voie dans la commune'],
    [ 1, 'cle_rivoli', 'Clé RIVOLI', 'Contrôle la validité de COMM'],
    [ 4, 'code_nature', 'Code nature de voie'],
    [26, 'libelle_voie', 'Libellé voie'],
    [ 1, FILLER],
    [ 1, 'type_comm', 'Type de la commune', 'N : rurale, R : recensée'],
    [ 2, FILLER],
    [ 1, 'caract_rur', 'Caractère RUR', '3 : pseudo-recensée, blanc sinon'],
    [ 2, FILLER],
    [ 1, 'caract_voie', 'Caractère de voie', '1 : privée, 0 : publique'],
    [ 1, 'caract_popul', 'Caractère de population', 'blanc si < 3000 hab, * sinon'],
    [ 9, FILLER],
    [ 7, 'popul_apart', 'Population à part'],
    [ 7, 'popul_fict', 'Population fictive'],
    [ 1, 'caract_annul', 'Caractère d’annulation', 'O : sans transfert, Q : avec'],
    [ 7, 'date_annul', 'Date d’annulation'],
    [ 7, 'date_crea', 'Date de création de l’article'],
    [15, FILLER],
    [ 5, 'code_majic', 'Code identifiant MAJIC de la voie'],
    [ 1, 'type_voie', 'Type de voie', '1 : voie, 2 : ensemble immobilier, 3 : lieu-dit, 4 :pseudo-voie, 5 : voie provisoire'],
    [ 1, 'caract_lieudit', 'Caractère du lieu-dit', '1 : lieu-dit bâti, 0 sinon'],
    [ 2, FILLER],
    [30, 'dernier_mot', 'Dernier mot entièrement alphabétique du libellé de la voie'],

]

enregistrement_final = [
    "final",
    [ 10, FILLER_9, '9999999999'],
    [  1, FILLER],
    [ 21, 'inconnu', '804187810937057800471'],    # non documenté, valeurs inconnues
    [118, FILLER_0, '0']
]


def decode(fields, line):
    """
    analyse une ligne selon la description fields
    retourne un dict si ok ou rien
    """
    row = {}
    offset = 0
    for field in fields[1:]:
        width, name = field[:2]
        value = line[offset:offset + width]
        if name == FILLER_ANY:
            pass
        elif name.startswith('_'):
            # space, 0, 9, etc.
            if value != name[1].encode() * width:
                return
        else:
            if value == '':
                print(line)
                assert False
                return
            row[name] = str.rstrip(value.decode())
        offset += width
    row['trailing'] = line[offset:]
    return row


def create(db, fields):
    """
    crée les tables de la base de données
    """
    sql = f"create table {fields[0]} (line number"
    for field in fields[1:]:
        width, name = field[:2]
        if not name.startswith("_"):
            sql += f", {name} text({width})"
    sql += ")"
    db.execute(sql)


def insert(db, fields, row, n):
    """
    insère un enregistrement dans la base de données
    """
    names = ['line']
    values = [n]
    for field in fields[1:]:
        width, name = field[:2]
        if not name.startswith("_"):
            names.append(name)
            values.append(row[name])

    sql = f"insert into {fields[0]} ({','.join(names)}) values ({','.join('?' * len(values))})"
    db.execute(sql, values)


def fantoir(archive):
    """
    itérateur sur chaque ligne du fichier contenu dans l'archive ZIP
    """
    zip = zipfile.ZipFile(archive)
    for i in zip.infolist():
        for line in zip.open(i.filename):
            yield line


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('archive', nargs='?', metavar='FANTOIR', help="Fichier national FANTOIR (archive)")
    args = parser.parse_args()

    if args.archive is None:
        z = glob.glob('FANTOIR*.zip')
        if len(z) != 1:
            parser.print_help()
            exit(2)
        args.archive = z[0]

    if os.path.exists("fantoir.sqlite"):
        os.unlink("fantoir.sqlite")
    db = sqlite3.connect("fantoir.sqlite")

    enregistrements = [enregistrement_final,
                       enregistrement_initial,
                       enregistrement_direction,
                       enregistrement_commune,
                       enregistrement_voie]

    for e in enregistrements:
        create(db, e)

    i = 1           # on commence par l'enregistrement initial
    n = 0           # compteur de ligne

    for line in tqdm(fantoir(args.archive), unit=" lignes", desc=args.archive):
        n += 1

        assert line[-2:] == b'\r\n'
        line = line[:-2]

        while True:
            row = decode(enregistrements[i], line)
            if row:
                # les enregistrements direction et commune ont la même longueur
                if enregistrements[i][0] == 'commune':
                    if row['code_comm'] == '':
                        i -= 1
                        continue

                # le décodeur courant fonctionne: on ajoute l'enregistrement à la base de données
                insert(db, enregistrements[i], row, n)

                # on passe au décodeur suivant
                if i < len(enregistrements) - 1:
                    i += 1
                break
            else:
                assert i > 0
                i -= 1

        if n % 10000 == 0:
            db.commit()

    db.commit()

    print("{} lignes lues".format(n))
    for fields in enregistrements:
        count = db.execute('select count(*) from ' + fields[0]).fetchone()[0]
        print("  {:<10} : {:>10}".format(fields[0], count))

    db.close()


if __name__ == '__main__':
    main()
