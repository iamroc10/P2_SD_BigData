from pandasql import sqldf
import pandas 
from lithops import Storage
import lithops
import matplotlib.pyplot as plt
import numpy

storage = Storage()
bucket_name = 'practica2-sd'


'''FUNCIÓ DE L'ETAPA 2, en que per paràmetre obté els diferents DataFrames que li ha retornat cada threads i els uneix amb un abans de retornar-lo'''
def mapreduce(results):
    df = results[0]
    df1 = results[1]
    df2 = results[2]
    df3 = results[3]
    dataFrame = pandas.concat([df,df1,df2,df3], axis=1)
    return dataFrame



'''FUNCIÓ DE L'ETAPA 2, en que cada thread obté del COS el seu fitxer per pre-processar-lo; un cop fet, el passa a tipus DataFrame i el retorna a la funció MAPREDUCE(results)'''
def data_preprocessing(fitxers, storage):
    parts = fitxers.split(':')
    nom = parts[0]
    fitxer = storage.get_object(bucket_name, nom)
    fitxer = str(fitxer)

    fitxer = fitxer[2:len(fitxer)-1]
    lines = fitxer.split('\\r\\n')
    line_0 = lines[0].split(';')
    
    dataF = pandas.DataFrame(columns=list(line_0))
    
    del lines[-1]
    del lines[0]

    for line in lines:
        line = line.split(';')
        dataF_len = len(dataF)
        dataF.loc[dataF_len] = line
    
    del parts[0]
    dataF2 = pandas.DataFrame(columns=list(parts))

    for part in parts:
        dataF2[part] = dataF[part]

    return dataF2
    


''' FUNCIÓ DE L'ETAPA 3, en que s'obté el fitxer del COS amb un GET i, cada thread, fa la seva consulta que hi ha en el paràmetre QUERY'''
def queries(query, storage):

    fitxer = storage.get_object(bucket_name, 'dades_CAT.csv')
    fitxer = str(fitxer)
    fitxer = fitxer[2:len(fitxer)-1]

    lines = fitxer.split('\\n')
    line_0 = lines[0].split(',')
    
    dataF = pandas.DataFrame(columns=list(line_0))
    
    del lines[-1]
    del lines[0]

    for line in lines:
        line = line.split(',')
        dataF_len = len(dataF)
        dataF.loc[dataF_len] = line
    
    parts = query.split(':')
    dataF[parts[1]] = pandas.to_numeric(dataF[parts[1]])

    result = sqldf(str(parts[0]), locals())  

    return(result)



''' FUNCIÓ en que passant-li per paràmetre VALUES, escriu el valor absolut en els gràfics PIE '''
def make_autopct(values):
    def my_autopct(pct):
        total = sum(values)
        val = int(round(pct*total/100.0))
        return'{v:d}'.format(p=pct,v=val)
    return my_autopct



''' FUNCIÓ esquelet per generar els gràfics de format PIE'''
def pie_plot(index, xlabel, ylabel, title):
    result_centres = result[index]
    langs = result_centres[xlabel]
    numb = result_centres[ylabel]
    fig = plt.figure()

    plt.pie(numb, labels=langs, autopct= make_autopct(numb))
    plt.suptitle(title, fontsize=14, fontweight='bold')
    plt.savefig("plot_"+ylabel+".png")



''' FUNCIÓ esquelet per generar els gràfics de format BAR'''
def bar_plot(index, xlabel, ylabel, title):
    result_centres = result[index]
    labels = result_centres[xlabel]
    fig = plt.figure()
    height = result_centres[ylabel]
    width = 0.3

    plt.bar(labels, height, width, color=['red', 'green', 'blue', 'purple'])
    plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
    plt.suptitle(title, fontsize=14, fontweight='bold')
    plt.savefig("plot_"+ylabel+".png")



''' FUNCIÓ esquelet per generar els gràfics de DISPERSIÓ'''
def dispersio_plot(index, xlabel, ylabel, title):
    result_centres = result[index]
    numb = result_centres[xlabel]
    numb = numpy.log10(numb)
    numb2 = result_centres[ylabel]
    numb2 = pandas.to_numeric(numb2)
    numb2 = numpy.log10(numb2)
    fig, aux = plt.subplots()
    aux.scatter(numb, numb2)
    
    plt.suptitle(title, fontsize=14, fontweight='bold')
    plt.savefig("plot_"+ylabel+"_"+xlabel+".png")




if __name__ == "__main__":
    
    fexec = lithops.FunctionExecutor(runtime='sd202021/lithops-custom-runtime-3.8:0.5')

    bucket_name = 'practica2-sd'
    obj_name = 'evolucio_covid_desembre20.csv'


    '''--------- FASE 2: DATA PREPROCESSING ---------''' 

    fitxers = [
'territori_ambiental.csv:COMARCA:TIPUS_COMARCA:SUPERFICIE',
'demografia.csv:HABITANTS:DENSITAT:POBLACIO_ESTRANGERA',
'infraestructura_sanitaria.csv:CENTRES:LLITS:FARMACIES:HAB_FARMACIA', 
'economia.csv:PIB:ATUR']

    fexec.map_reduce(data_preprocessing, fitxers, mapreduce)
    result = fexec.get_result()
    result.to_csv(r'export_dataFrame.csv', index=False)

    f= open('export_dataFrame.csv', 'r')
    text = f.read()
    storage.put_object(bucket_name, key='dades_CAT.csv', body = text)  
    f.close()



    '''--------- FASE 3: QUERIES ---------'''

    iterdata = [
'SELECT dataF.COMARCA AS COMARCA, dataF.CENTRES AS CENTRES FROM dataF GROUP BY dataF.COMARCA ORDER BY dataF.CENTRES DESC LIMIT 6:CENTRES', 

'SELECT dataF.COMARCA AS COMARCA, dataF.HABITANTS AS HABITANTS FROM dataF GROUP BY dataF.COMARCA ORDER BY dataF.HABITANTS DESC LIMIT 6:HABITANTS', 

'SELECT dataF.COMARCA AS COMARCA, dataF.FARMACIES AS FARMACIES FROM dataF GROUP BY dataF.COMARCA ORDER BY dataF.FARMACIES DESC LIMIT 4:FARMACIES',

'SELECT dataF.COMARCA AS COMARCA, dataF.PIB AS PIB FROM dataF GROUP BY dataF.COMARCA ORDER BY dataF.PIB DESC LIMIT 4:PIB',

'SELECT dataF.COMARCA AS COMARCA, dataF.DENSITAT AS DENSITAT FROM dataF GROUP BY dataF.COMARCA ORDER BY dataF.DENSITAT DESC LIMIT 6:DENSITAT',

"SELECT dataF.COMARCA AS COMARCA, dataF.POBLACIO_ESTRANGERA AS POBLACIO_ESTRANGERA FROM dataF WHERE dataF.COMARCA='BARCELONES' or dataF.COMARCA='VALLES OCCIDENTAL' GROUP BY dataF.COMARCA ORDER BY dataF.POBLACIO_ESTRANGERA DESC:POBLACIO_ESTRANGERA",

'SELECT dataF.PIB AS PIB, dataF.ATUR AS ATUR FROM dataF GROUP BY dataF.COMARCA:PIB',

'SELECT dataF.DENSITAT AS DENSITAT, dataF.HABITANTS AS HABITANTS FROM dataF GROUP BY dataF.COMARCA:DENSITAT',

'SELECT dataF.TIPUS_COMARCA AS TIPUS_COMARCA, COUNT(dataF.TIPUS_COMARCA) AS COUNT_TIPUS_C FROM dataF GROUP BY dataF.TIPUS_COMARCA:PIB'
]

    fexec.map(queries, iterdata)
    result = fexec.get_result()


    '''--------- FASE 3.1: PLOTS ---------'''

    pie_plot(0, 'COMARCA', 'CENTRES', 'COMARQUES AMB MÉS CENTRES HOSPITALARIS CAT 2020')
    pie_plot(1, 'COMARCA', 'HABITANTS', 'COMARQUES AMB MÉS HABITANTS CAT 2020')

    bar_plot(2, 'COMARCA', 'FARMACIES', 'COMARQUES AMB MÉS FARMÀCIES CATALUNYA 2020')
    bar_plot(3, 'COMARCA', 'PIB', 'COMARQUES AMB MÉS PIB CATALUNYA 2020')

    pie_plot(4, 'COMARCA', 'DENSITAT', 'COMARQUES AMB MÉS DENSITAT CAT 2020')
    pie_plot(5, 'COMARCA', 'POBLACIO_ESTRANGERA', '% POBLACIO ESTRANGERA COMARQUES MÉS POBLADES')

    dispersio_plot(6, 'PIB', 'ATUR', 'GRÀFIC DISPERSIÓ PIB vs ATUR CATALUNYA')
    dispersio_plot(7, 'DENSITAT', 'HABITANTS', 'GRÀFIC DISPERSIÓ DENSITAT vs HABITANTS CATALUNYA')

    bar_plot(8, 'TIPUS_COMARCA','COUNT_TIPUS_C', 'TIPUS DE COMARQUES A CATALUNYA')

