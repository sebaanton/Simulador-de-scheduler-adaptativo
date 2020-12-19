import time
import numpy as np
from scipy import stats

class Nodo:
    def __init__(self, tipo, id_nodo, capacidad_procesamiento, ocupacion, trabajo_max): #tag = ""):
        self.tipo = tipo #spout/bolt
        self.id_nodo = id_nodo
        self.capacidad_procesamiento_fija = capacidad_procesamiento
        self.capacidad_actual_p = capacidad_procesamiento
        self.ocupacion = ocupacion
        self.trabajo = 0
        self.trabajo_max = trabajo_max
        self.trabajo_max_actual = trabajo_max
        self.proce_total_nodos = []
    
    def get_tipo(self):
        return self.tipo
    
    def get_id(self):
        return self.id_nodo

    def get_capacidad_p_fija(self):
        return self.capacidad_procesamiento_fija # capacidad de procesamiento del nodo, 10 datos/seg

    def get_capacidad_p_actual(self):
        return self.capacidad_actual_p
    
    def set_capacidad_p(self, entrada): # 0 sumar y 1 resta capacidades al aumentar o disminuir nodos
        if(entrada == 0):
            self.capacidad_actual_p += self.capacidad_procesamiento_fija
        else:
            self.capacidad_actual_p -= self.capacidad_procesamiento_fija
    
    def get_ocupacion(self):
        return self.ocupacion #lo que hace el bolt/spout (string)

    def get_trabajo(self): # el trabajo ocupa cierta capacidad de procesamiento
        return self.trabajo
    
    def get_trabajo_max(self): # Buffer que indica la cantidad máxima de tweets que puede recibir
        return self.trabajo_max
    
    def get_trabajo_max_actual(self): 
        return self.trabajo_max_actual
    
    def set_trabajo_max_actual(self, entrada): # 0 sumar y 1 resta capacidades al aumentar o disminuir nodos
        if(entrada == 0):
            self.trabajo_max_actual += self.trabajo_max
        else:
            self.trabajo_max_actual -= self.trabajo_max
        
    def set_trabajo(self, t_entrante): #
        self.trabajo += t_entrante # suma o resta trabajo
    
    def set_proce_total_nodos(self, proce):
        self.proce_total_nodos.append(proce)
        
    def get_proce_total_nodos(self):
        return self.proce_total_nodos

    
class Topologia:
    # este hace que los nodos se comuniquen entre si
    def __init__(self):
        self.matriz = []
        self.activacion_monitoreo = True
        self.datos = [] 
        
    def get_activacion_monitoreo(self):
        return self.activacion_monitoreo
    
    def set_activacion_monitoreo(self, valor): # valor True o False
        self.activacion_monitoreo = valor
        
    def get_matriz(self):
        return self.matriz
        
    def conexion_nodos(self, nodo_1, nodo_2):
        if(len(self.matriz) <= nodo_1.get_id()):
            self.matriz.append([nodo_1])
            
        self.matriz[nodo_1.get_id()].append(nodo_2)
        nodo_1.set_proce_total_nodos(nodo_2.get_capacidad_p_actual())
    
    def duplicar_nodo(self, nodo): # 0 sumar y 1 resta capacidades al aumentar o disminuir nodos
        nodo.set_capacidad_p(0)
        nodo.set_trabajo_max_actual(0)
            
    def reducir_nodo(self, nodo): # 0 sumar y 1 resta capacidades al aumentar o disminuir nodos
        if(nodo.get_trabajo_max_actual() > nodo.get_trabajo_max()):
            nodo.set_capacidad_p(1)
            nodo.set_trabajo_max_actual(1)
            return 1
        else:
            print("No se puede reducir más")
            return 0
        
    def monitoreo(self, trabajo, nodo, activacion):
        if(activacion):
            if (trabajo > nodo.get_trabajo_max_actual()):
                while(trabajo > nodo.get_trabajo_max_actual()):
                    self.duplicar_nodo(nodo)
                    print("Se duplica el nodo de ID: " + str(nodo.get_id()))
                self.guardar_datos(trabajo, nodo)
            else:
                while(trabajo < nodo.get_trabajo_max_actual()):
                    algo = self.reducir_nodo(nodo)
                    print("Se divide el nodo de ID: " + str(nodo.get_id()))
                    print("algo " + str(algo))
                    if(algo == 0):
                        if(trabajo > nodo.get_trabajo_max_actual()): 
                            self.duplicar_nodo(nodo)
                            print("Se vuelve a multiplicar el nodo de ID: " + str(nodo.get_id()) + " por quedar corto")
                            break
                        else:
                            break
                    elif(trabajo > nodo.get_trabajo_max_actual()): 
                            self.duplicar_nodo(nodo)
                            print("Se vuelve a multiplicar el nodo de ID: " + str(nodo.get_id()) + " por quedar corto")
                            break
                        
    def guardar_datos(self, trabajo_entrada, nodo): 
        multiplicaciones = nodo.get_trabajo_max_actual() / nodo.get_trabajo_max()
        print("Con el trabajo entrante de: " + str(trabajo_entrada) + " el nodo de ID: " + str(nodo.get_id()) + " tuvo que multiplicarse " + str(multiplicaciones) + " veces. Este nodo tiene la capacidad de procesamiento inicial de: " \
            + str(nodo.get_capacidad_p_fija()) + " y luego de multiplicarse alcanza la capacidad de: " + str(nodo.get_capacidad_p_actual()) + ". Este parte con un trabajo máximo inicial de: " + \
                str(nodo.get_trabajo_max()) + " llegando a alcanzar un trabajo máximo total de: " + str(nodo.get_trabajo_max_actual()) + " luego de multiplicarse.")
        self.datos.append([trabajo_entrada, nodo.get_id(), multiplicaciones, nodo.get_capacidad_p_fija(), nodo.get_capacidad_p_actual(), nodo.get_trabajo_max(), nodo.get_trabajo_max_actual()])
        
    def get_datos(self): 
        return self.datos
        
    def enviar_trabajo(self, nuevo_trabajo): # hacer esta wea con recursividad (ya no :P)
        contador = 0
        trabajo_actual = 0
        self.monitoreo(nuevo_trabajo, self.matriz[0][0], self.get_activacion_monitoreo()) 
        for i in self.matriz:
            proce_total = 0
            print("Iteración: " + str(contador))
            print("nuevo_trabajo: " + str(nuevo_trabajo))
            print("len_i: " + str(len(i)))
            
            for k in range(1, len(i)): 
                proce_total = proce_total + i[k].get_trabajo_max_actual()
                
            if(contador == 0):
                trabajo_actual = nuevo_trabajo
                i[0].set_trabajo(nuevo_trabajo)
            else:
                trabajo_actual = i[0].get_trabajo()
            for j in range(1, len(i)):
                print("trabajo_actual: " + str(trabajo_actual) )
                print("get_trabajo_max_actual: " + str(i[j].get_trabajo_max_actual()) + " del nodo " + str(i[j].get_id()))
                print("proce_total: " + str(proce_total))

                trabajo_proporcionado = (i[j].get_trabajo_max_actual() * trabajo_actual) / proce_total # distribuye el trabajo en los nodos de manera proporcionada, en base al tamaño del buffer
                print("trabajo_proporcionado: " +  str(trabajo_proporcionado))

                print("get_trabajo_max_actual: " + str(i[j].get_trabajo_max_actual()) + " del nodo " + str(i[j].get_id()))
                
                self.monitoreo(trabajo_proporcionado, i[j], self.get_activacion_monitoreo())
                
                
                print("get_trabajo_max_actual: " + str(i[j].get_trabajo_max_actual()) + " del nodo " + str(i[j].get_id()))
                tiempo = trabajo_proporcionado / i[j].get_capacidad_p_actual()
                i[j].set_trabajo(trabajo_proporcionado)
                #print("El " + str(i[j].get_tipo())+  " recibe " + str(i[j].get_trabajo()) + " trabajo") # print trabajo en procesamiento actual
                print("El " + str(i[j].get_tipo())+  " recibe " + str(trabajo_proporcionado) + " trabajo") # print trabajo actual
                #time.sleep(tiempo)
            
            i[0].set_trabajo(trabajo_actual*-1)
            print("Trabajo actual del nodo " + str(i[0].get_id()) + ": " + str(i[0].get_trabajo()))
            contador += 1
            print("\n")

def aleatorio(cantidad_datos):
    mu, sigma = 0, 0.2 # media y desviación estándar
    normal = stats.norm(mu, sigma)
    x = np.linspace(normal.ppf(0.01),
                normal.ppf(0.99), 100)
    fp = normal.pdf(x) # Función de Probabilidad
    for i in range(0,len(fp)):
        fp[i]=fp[i]*300
    return fp

nodo1 = Nodo("Spout", 0, 10, "Recibe tweets", 100)
nodo2 = Nodo("Bolt 1", 1, 8, "Recibe del spout 0 y separa por región", 100)
nodo3 = Nodo("Bolt 2", 2, 5, "Recibe del spout 0 y separa por región", 100)
nodo4 = Nodo("Bolt 3", 3, 6, "Recibe los tweets separados por region y separa palabras", 100)
nodo5 = Nodo("Bolt 4", 4, 15, "Recibe los tweets separados por region y separa palabras", 100)
nodo6 = Nodo("Bolt 5", 5, 30, "Recibe las palabras separadas y las guarda en la base de datos", 300)

nueva_topologia = Topologia()
nueva_topologia.conexion_nodos(nodo1, nodo2)
nueva_topologia.conexion_nodos(nodo1, nodo3)
nueva_topologia.conexion_nodos(nodo2, nodo4)
nueva_topologia.conexion_nodos(nodo2, nodo5)
nueva_topologia.conexion_nodos(nodo3, nodo4)
nueva_topologia.conexion_nodos(nodo3, nodo5)
nueva_topologia.conexion_nodos(nodo4, nodo6)
nueva_topologia.conexion_nodos(nodo5, nodo6)

"""
            Bolt 1      ----    Bolt 3   
        /            \     /             \
spout                                       Bolt 5
        \            /    \              /
            Bolt 2      ----    Bolt 4      

"""


matriz = nueva_topologia.get_matriz()

#nueva_topologia.enviar_trabajo(1000) # prueba con entrada de 1000 tweets para la topología de ejemplo

y = aleatorio(100) # Se generan 100 numeros aleatorios (simulando 100 cantidades de tweets en 100 unidades de tiempo [ejemplo: llegan 500 tweets en el minuto 1, 600 tweets en el minuto 2, etc]) con distribución normal

trabajo = []

for j in range(0,20):
    if(j%5 == 0):#Guardar los datos cada ciertos instantes de tiempo, em este casp se simula que se guardan cada 5 unidades de tiempo
        trabajo.append(y[j])
        if(y[j] > 0.75 * matriz[0][0].get_trabajo_max_actual()): # Si sobre pasa el 75% de procesamiento del nodo inicial, se activa el monitoreo
            nueva_topologia.set_activacion_monitoreo(True)
        else:
            nueva_topologia.set_activacion_monitoreo(False)
    nueva_topologia.enviar_trabajo(y[j])

print(nueva_topologia.get_datos())
print(trabajo)
