# -*- coding: utf-8 -*-
"""
Created on Sat May  6 12:16:33 2023

@author: USUARIO
"""

'''
                    APARTADO 1 PRÁCTICA TRICICLO
En el archivo de las aristas, estas tiene que estar de la forma (x,y) y una en cada linea.
Como ejemplo, se adjunta un archivo en la misma carpeta que esta práctica con aristas. 

'''


from pyspark import SparkContext
import sys


#Funcion que dado un par de nodos, devuelve la tupla ordenada.
#Ejemplo: ordenar_nodos((5,1)) = (1,5)
def ordenar_nodos(nodo):
    nodo0 = nodo[1]
    nodo1 = nodo[3]
    if nodo0 < nodo1:
        return (nodo0,nodo1)
    else:
        return (nodo1,nodo0)
    
#Funcion que dada una tupla de un nodo y sus contiguos, devuelve los pares existentes y los
#pares pendientes. Esta función realiza la pista numero 2 del pdf de triciclos
def juntar(tupla):
    nodo_princ = tupla[0]
    nodos_sec = tupla[1]
    matriz = []
    for i in range(len(nodos_sec)):
        nodo1 = nodos_sec[i]
        matriz.append(  ((nodo_princ,nodo1), 'exists')  )
        for j in range(i,len(nodos_sec)):
            nodo2 = nodos_sec[j]
            if nodo1 == nodo2:
                pass
            elif nodo1 < nodo2:
                matriz.append( ((nodo1,nodo2),('pending',nodo_princ)) )
            else:
                matriz.append( ((nodo2,nodo1),('pending',nodo_princ)) )
    return (nodo_princ,matriz)
                
#Función que dada una tupla, une los componentes del segundo elemento de la tupla 
#al primer elemento de esta tupla en el caso en el que la componente esté pendiente.
#Ejemplo: unir_adj( (('2', '3'), [('pending', '1'), ('pending', '2'), 'exists']) ) = 
# = ('2', '3', '1', '2')
def unir_adj(tupla):
    nodo_princ = tupla[0]
    nodos_sec = tupla[1]
    for nodo in nodos_sec:
        if nodo != 'exists':
            nodo_princ += (nodo[1],)
    return nodo_princ

#Función que dada una lista, devuelve la misma lista pero sin elementos repetidos
def distintos(lista):
    new = []
    for i in lista:
        if i not in new:
            new.append(i)
    return new
    
    
#Función principal del programa
def main(texto):
    sc = SparkContext()
    lista_nodos = sc.textFile(texto)
    rdd0 = lista_nodos.map(ordenar_nodos).distinct()
    rdd1 = rdd0.groupByKey().mapValues(list)
    rdd2 = rdd1.map(juntar)
    rdd3 = rdd2.flatMap(lambda x: tuple(x[1]))
    rdd4 = rdd3.groupByKey().mapValues(list)
    rdd5 = rdd4.filter(lambda x: len(x[1])>1)
    rdd6 = rdd5.mapValues(list)
    rdd7 = rdd6.map(unir_adj)
    rdd8 = rdd7.map(distintos)
    rdd9 = rdd8.filter(lambda x: len(x) == 3)
    print(rdd9.collect())
    sc.stop()


if __name__ == '__main__':
    main(sys.argv[1])