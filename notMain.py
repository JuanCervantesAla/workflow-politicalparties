from prefect import flow, task

@task#Just a function
def hello_world():
    print("Hola para la clase del profe Michel")
    return ("Hola clasesota")#Returns an string

@task
def prefect_say(s: str):#A function(Task receiving an string)
    print(s)

@flow
def my_first_flow():#Creates the flow
    r = hello_world() #Gets the return data from helloworld
    s2 = prefect_say(r)#prints the data from hellworld

if __name__ == "__main__":
    my_first_flow()  # Executes the flow wich contains my function
