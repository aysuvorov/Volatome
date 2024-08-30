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


## Merging Volatomes

I have 2 tables:

| 42_03257085331836 | 43_04741898699762 | 44_01450529149254 | 44_0489926172728 | 44_99144370755021 | PatientID | ProbeId    | DateTime                   | target_short_name | target_icd_10 | target_icd_11 |
|------------------:|------------------:|------------------:|-----------------:|------------------:|----------:|:-----------|:---------------------------|:------------------|:--------------|:--------------|
|         0.0347108 |          0.289125 |         0.0396336 |        0.0231194 |           3.52877 |     17558 | ProbeId_2  | 2024-08-30 19:24:57.900720 | ИБС            | I20.8         | BA40.1        |
|          0.936501 |           1.91298 |         0.0739828 |         0.087051 |           3.51884 |     21569 | ProbeId_23 | 2024-08-30 19:24:57.900740 | ИБС            | I20.8         | BA40.1        |
|         0.0703742 |           2.49884 |         0.0789201 |         0.101689 |           2.68527 |     21094 | ProbeId_74 | 2024-08-30 19:24:57.900792 | Healthy           | Healthy       | Healthy       |
|         0.0917151 |           1.27432 |         0.0940473 |        0.0762481 |           5.33148 |     19907 | ProbeId_20 | 2024-08-30 19:24:57.900737 | ИБС            | I20.8         | BA40.1        |
|          0.741693 |          0.964586 |         0.0550934 |        0.0501778 |           3.35456 |     20195 | ProbeId_60 | 2024-08-30 19:24:57.900777 | Healthy           | Healthy       | Healthy       |
|          0.074873 |          0.811691 |         0.0709599 |        0.0548712 |            3.9698 |     21387 | ProbeId_71 | 2024-08-30 19:24:57.900788 | Healthy           | Healthy       | Healthy       |
|          0.106864 |           3.14315 |         0.0991804 |         0.139365 |           4.15313 |     20007 | ProbeId_14 | 2024-08-30 19:24:57.900732 | ИБС            | I20.8         | BA40.1        |
|          0.171386 |           3.38432 |         0.0795997 |         0.137908 |           2.83617 |     18380 | ProbeId_51 | 2024-08-30 19:24:57.900767 | Healthy           | Healthy       | Healthy       |

and 

| 44_99144370755021 | 45_99197266970869 | 47_04077398412186 | 48_045099177099885 | 49_00477743760921 | 49_99501251131184 | PatientID | ProbeId    | DateTime                   | target_short_name | target_icd_10 | target_icd_11 |
|------------------:|------------------:|------------------:|-------------------:|------------------:|------------------:|----------:|:-----------|:---------------------------|:------------------|:--------------|:--------------|
|           2.97995 |         0.0584557 |           8.01259 |           0.178858 |         0.0308905 |        0.00896429 |     22015 | ProbeId_76 | 2024-08-30 19:25:48.903608 | Healthy           | Healthy       | Healthy       |
|           1.92739 |         0.0499004 |          0.192831 |         0.00517879 |         0.0379916 |        0.00480427 |     23843 | ProbeId_79 | 2024-08-30 19:25:48.903611 | Healthy           | Healthy       | Healthy       |
|           2.68572 |         0.0527857 |           3.64771 |          0.0876438 |         0.0344578 |         0.0064166 |     21869 | ProbeId_21 | 2024-08-30 19:25:48.903569 | ИБС            | I20.8         | BA40.1        |
|           4.15313 |         0.0827267 |           6.01692 |           0.142089 |         0.0984609 |         0.0137742 |     20007 | ProbeId_14 | 2024-08-30 19:25:48.903565 | ИБС            | I20.8         | BA40.1        |
|           4.53717 |         0.0946463 |          0.298547 |          0.0110313 |         0.0895525 |          0.011893 |     21925 | ProbeId_25 | 2024-08-30 19:25:48.903572 | ИБС            | I20.8         | BA40.1        |
|           3.51884 |         0.0708363 |          0.166848 |         0.00590104 |         0.0090919 |        0.00815203 |     21569 | ProbeId_23 | 2024-08-30 19:25:48.903571 | ИБС            | I20.8         | BA40.1        |
|           2.65582 |          0.046235 |          0.119985 |         0.00359185 |         0.0695031 |        0.00597724 |     17508 | ProbeId_32 | 2024-08-30 19:25:48.903577 | Healthy           | Healthy       | Healthy       |
|           3.97766 |         0.0668665 |          0.130496 |         0.00457821 |         0.0493458 |        0.00639664 |     17501 | ProbeId_35 | 2024-08-30 19:25:48.903579 | Healthy           | Healthy       | Healthy       |

I need to: 

- create from them 2 objects of class VolatomeVoid and 
- to merge them on Volatome.unique_id values from both VolatomeVoid objects

resulting object of class VolatomeVoid I need to save as JSON

original code:

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

    def to_json(self, filename: str):
        with open(filename, 'w') as f:
            json.dump({uid: volatome.__dict__ for uid, volatome in self.volatome_storage.items()}, f, default=str)

    @classmethod
    def from_json(cls, filename: str):
        with open(filename, 'r') as f:
            data = json.load(f)
            volatome_void = cls()
            for uid, volatome_data in data.items():
                volatome_data['collection_time'] = pd.to_datetime(volatome_data['collection_time'])
                volatome = Volatome(**volatome_data)
                volatome_void.add_volatome(volatome)
            return volatome_void

    def search(self, **kwargs):
        results = []
        for volatome in self.volatome_storage.values():
            match = True
            for key, value in kwargs.items():
                if hasattr(volatome, key):
                    if getattr(volatome, key) != value:
                        match = False
                elif key == 'targets':
                    if not any(t.short_name == value for t in volatome.targets):
                        match = False
                elif key == 'voc_objects':
                    if not any(v.identifier == value for v in volatome.voc_objects):
                        match = False
            if match:
                results.append(volatome)
        return results

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

    def to_json(self, filename: str):
        with open(filename, 'w') as f:
            json.dump({(target.icd_10, target.icd_11): target.__dict__ for target in self.target_storage.values()}, f)

    @classmethod
    def from_json(cls, filename: str):
        with open(filename, 'r') as f:
            data = json.load(f)
            target_void = cls()
            for (icd_10, icd_11), target_data in data.items():
                target = Target(**target_data)
                target_void.add_target(target)
            return target_void

    def search(self, short_name: Optional[str] = None, icd_10: Optional[str] = None, icd_11: Optional[str] = None):
        results = []
        for target in self.target_storage.values():
            if (short_name is None or target.short_name == short_name) and \
               (icd_10 is None or target.icd_10 == icd_10) and \
               (icd_11 is None or target.icd_11 == icd_11):
                results.append(target)
        return results

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

    def to_json(self, filename: str):
        with open(filename, 'w') as f:
            json.dump({person_id: person.__dict__ for person_id, person in self.person_storage.items()}, f)

    @classmethod
    def from_json(cls, filename: str):
        with open(filename, 'r') as f:
            data = json.load(f)
            person_void = cls()
            for person_id, person_data in data.items():
                person = Person(**person_data)
                person_void.add_person(person)
            return person_void

    def search(self, person_id: Optional[str] = None):
        results = []
        for person in self.person_storage.values():
            if person_id is None or person.person_id == person_id:
                results.append(person)
        return results

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
