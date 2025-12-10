


# dictionary with name changes
#-----------------------------

alt_fin = {
    f'Alt fin brut {i} granulés fin P{j}':f'Alt brut {i} granulés fin P{j}'
    for i in range(1, 3) for j in range(3, 9)
    
}

nb_Anim = {
    f'Nb animaux {name} P{i}':f'Nb Animaux {name} P{i}'
    for i in range(1, 9) for name in ('début', 'fin')
}

dict_rename = alt_fin | nb_Anim

# consider only features with raw data
# exclude calculations and secondary additions...
#---------------------------------------------------------------
col_exclude = [
    'I.C.', 
    'IC', 
    'GP', 
    'PV', 
    'Conso', 
    'éliminés', 
    'Remarques', 
    'Unnam', 
    'moyen',
    'Nb Animaux fin', #calculation from mortalité
    'Nb animaux fin',
    'Nb Animaux début', #calculation from mortalité
    'prélevé', #include because of new studies Jun23
    'sorti', #include because of new studies Jun23
    'retiré', #include because of new studies Jun23
    'rééquilibré', #include because of new studies Jun23
    'poulet'
]