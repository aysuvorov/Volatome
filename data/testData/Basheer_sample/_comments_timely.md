### Data Loading and creation


I have a table:

| 42_03257085331836 | 43_04741898699762 | 44_01450529149254 | 44_0489926172728 | 44_99144370755021 | PatientID | ProbeId    | DateTime                   | target_short_name | target_icd_10 | target_icd_11 |
|------------------:|------------------:|------------------:|-----------------:|------------------:|----------:|:-----------|:---------------------------|:------------------|:--------------|:--------------|
|         0.0347108 |          0.289125 |         0.0396336 |        0.0231194 |           3.52877 |     17558 | ProbeId_2  | 2024-08-30 18:37:04.301985 | ИБС            | I20.8         | BA40.1        |
|          0.936501 |           1.91298 |         0.0739828 |         0.087051 |           3.51884 |     21569 | ProbeId_23 | 2024-08-30 18:37:04.302000 | ИБС            | I20.8         | BA40.1        |
|         0.0703742 |           2.49884 |         0.0789201 |         0.101689 |           2.68527 |     21094 | ProbeId_74 | 2024-08-30 18:37:04.302043 | Healthy           | Healthy       | Healthy       |
|         0.0917151 |           1.27432 |         0.0940473 |        0.0762481 |           5.33148 |     19907 | ProbeId_20 | 2024-08-30 18:37:04.301998 | ИБС            | I20.8         | BA40.1        |
|          0.741693 |          0.964586 |         0.0550934 |        0.0501778 |           3.35456 |     20195 | ProbeId_60 | 2024-08-30 18:37:04.302033 | Healthy           | Healthy       | Healthy       |
|          0.074873 |          0.811691 |         0.0709599 |        0.0548712 |            3.9698 |     21387 | ProbeId_71 | 2024-08-30 18:37:04.302040 | Healthy           | Healthy       | Healthy       |
|          0.106864 |           3.14315 |         0.0991804 |         0.139365 |           4.15313 |     20007 | ProbeId_14 | 2024-08-30 18:37:04.301994 | ИБС            | I20.8         | BA40.1        |
|          0.171386 |           3.38432 |         0.0795997 |         0.137908 |           2.83617 |     18380 | ProbeId_51 | 2024-08-30 18:37:04.302026 | Healthy           | Healthy       | Healthy       |


I have a code of objects and structures:

```
@dataclass
class VOC:
    identifier: str = 'unknown'
    value: Optional[float] = None
    method: str = 'Silantiev_maximization'
    chemical_formula: Optional['Substance'] = None

@dataclass
class Substance:
    voc_identifiers: Union[str, List[str]] = 'unknown'
    breath_type: str = 'any'
    chemical_formula: Optional[str] = 'unknown'

@dataclass
class Target:
    short_name: str
    icd_10: str
    icd_11: str

@dataclass
class Person:
    person_id: str

@dataclass
class Volatome:
    unique_id: str
    person: Person  # Use the Person class instead of a string for person_id
    collection_time: datetime = field(default_factory=datetime.now)
    breath_type: str = 'normal'
    normalization: Optional[str] = None
    targets: List[Target] = field(default_factory=list)
    voc_objects: List[VOC] = field(default_factory=list)

@dataclass
class VolatomeVoid:
    volatome_storage: dict = field(default_factory=dict)

    def add_volatome(self, volatome: Volatome):
        self.volatome_storage[volatome.unique_id] = volatome

    def get_volatome(self, unique_id: str) -> Optional[Volatome]:
        return self.volatome_storage.get(unique_id, None)

    def remove_volatome(self, unique_id: str):
        if unique_id in self.volatome_storage:
            del self.volatome_storage[unique_id]

@dataclass
class TargetVoid:
    target_storage: dict = field(default_factory=dict)

    def add_target(self, target: Target):
        """Add a Target object to the storage."""
        self.target_storage[(target.icd_10, target.icd_11)] = target

    def get_target(self, icd_10: str, icd_11: str) -> Optional[Target]:
        """Retrieve a Target object by its icd codes."""
        return self.target_storage.get((icd_10, icd_11), None)

    def remove_target(self, icd_10: str, icd_11: str):
        """Remove a Target object from the storage."""
        if (icd_10, icd_11) in self.target_storage:
            del self.target_storage[(icd_10, icd_11)]

@dataclass
class PersonVoid:
    person_storage: dict = field(default_factory=dict)

    def add_person(self, person: Person):
        """Add a Person object to the storage."""
        self.person_storage[person.person_id] = person

    def get_person(self, person_id: str) -> Optional[Person]:
        """Retrieve a Person object by its person_id."""
        return self.person_storage.get(person_id, None)

    def remove_person(self, person_id: str):
        """Remove a Person object from the storage."""
        if person_id in self.person_storage:
            del self.person_storage[person_id]
```

In the table: 

- every cell for columns 42_03257085331836 | 43_04741898699762 | 44_01450529149254 | 44_0489926172728 | 44_99144370755021 are VOC values
  
- every column names in 42_03257085331836 | 43_04741898699762 | 44_01450529149254 | 44_0489926172728 | 44_99144370755021 are VOC identifiers
  
- PatinentID column represents Person class, values are Person.person_id, Persons are stored in PersonVoid
  
- target_short_name column values are short names of class Target objects, targets are stored in TargetVoid
- target_icd_10 column values are icd_10 values of class Target objects
- target_icd_11 column values are icd_11 values of class Target objects
  
- every row is class Volatome object with:
  - unique_id - value in ProbeID column
  - person - value in PersonID column - object of class Person
  - collection_time - value in DateTime column
  - normalization = 'raw'
  - targets - values from target column - short names of Target objects
  - voc_objects - objects of class VOC (column names - VOC identifiers, cell values - values in class VOC objects)
  
- all the objects of Volatome class are parts of VolatomeVoid class object

Also add DataLoader class that is a pipeline to load data from the table into VolatomeVoid, PersonVoid, TargetVoid objects

### Data storage as JSONs

I have objects:

```
@dataclass
class VOC:
    identifier: str = 'unknown'
    value: Optional[float] = None
    method: str = 'Silantiev_maximization'
    chemical_formula: Optional['Substance'] = None

@dataclass
class Substance:
    voc_identifiers: Union[str, List[str]] = 'unknown'
    breath_type: str = 'any'
    chemical_formula: Optional[str] = 'unknown'

@dataclass
class Target:
    short_name: str
    icd_10: str
    icd_11: str

@dataclass
class Person:
    person_id: str

@dataclass
class Volatome:
    unique_id: str
    person: Person  # Use the Person class instead of a string for person_id
    collection_time: datetime = field(default_factory=datetime.now)
    breath_type: str = 'normal'
    normalization: Optional[str] = 'raw'
    targets: List[Target] = field(default_factory=list)
    voc_objects: List[VOC] = field(default_factory=list)

@dataclass
class VolatomeVoid:
    volatome_storage: dict = field(default_factory=dict)

    def add_volatome(self, volatome: Volatome):
        self.volatome_storage[volatome.unique_id] = volatome

    def get_volatome(self, unique_id: str) -> Optional[Volatome]:
        return self.volatome_storage.get(unique_id, None)

    def remove_volatome(self, unique_id: str):
        if unique_id in self.volatome_storage:
            del self.volatome_storage[unique_id]

@dataclass
class TargetVoid:
    target_storage: dict = field(default_factory=dict)

    def add_target(self, target: Target):
        """Add a Target object to the storage."""
        self.target_storage[(target.icd_10, target.icd_11)] = target

    def get_target(self, icd_10: str, icd_11: str) -> Optional[Target]:
        """Retrieve a Target object by its icd codes."""
        return self.target_storage.get((icd_10, icd_11), None)

    def remove_target(self, icd_10: str, icd_11: str):
        """Remove a Target object from the storage."""
        if (icd_10, icd_11) in self.target_storage:
            del self.target_storage[(icd_10, icd_11)]

@dataclass
class PersonVoid:
    person_storage: dict = field(default_factory=dict)

    def add_person(self, person: Person):
        """Add a Person object to the storage."""
        self.person_storage[person.person_id] = person

    def get_person(self, person_id: str) -> Optional[Person]:
        """Retrieve a Person object by its person_id."""
        return self.person_storage.get(person_id, None)

    def remove_person(self, person_id: str):
        """Remove a Person object from the storage."""
        if person_id in self.person_storage:
            del self.person_storage[person_id]

class DataLoader:
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe
        self.volatome_void = VolatomeVoid()
        self.person_void = PersonVoid()
        self.target_void = TargetVoid()

    def load_data(self):
        # Load targets into TargetVoid
        for index, row in self.dataframe.iterrows():
            target = Target(
                short_name=row['target_short_name'],
                icd_10=row['target_icd_10'],
                icd_11=row['target_icd_11']
            )
            self.target_void.add_target(target)

        # Load persons into PersonVoid
        for person_id in self.dataframe['PatientID'].unique():
            person = Person(person_id=str(person_id))
            self.person_void.add_person(person)

        # Load Volatome objects into VolatomeVoid
        for index, row in self.dataframe.iterrows():
            person = self.person_void.get_person(str(row['PatientID']))
            if person:
                # Create VOC objects from the row
                voc_objects = [
                    VOC(identifier=col, value=row[col]) 
                    for col in self.dataframe.columns[:-5]  # Exclude PatientID, ProbeId, DateTime, target_short_name, target_icd_10, target_icd_11
                ]
                
                # Create targets from the target column
                targets = [self.target_void.get_target(row['target_icd_10'], row['target_icd_11'])]

                # Create Volatome object
                volatome = Volatome(
                    unique_id=row['ProbeId'],
                    person=person,
                    collection_time=row['DateTime'],
                    targets=targets,
                    voc_objects=voc_objects
                )
                
                # Add Volatome to VolatomeVoid
                self.volatome_void.add_volatome(volatome)

```

Add additional code / rewrite the existing code:

- VolatomeVoid can be saved as json file, loaded, searched and filtered by:
  - person
  - collection_time
  - breath_type
  - normalization
  - targets
  - VOC identifiers
  - Substance by chemical_formula

- TargetVoid can be saved as json file, loaded, searched and filtered by:
  - short_name
  - icd_10
  - icd_11

- PersonVoid can be saved as json file, loaded, searched and filtered by:
  - person_id

Add additional code / rewrite the existing code:

- different VolatomeVoid objects can be merged together by person, 
