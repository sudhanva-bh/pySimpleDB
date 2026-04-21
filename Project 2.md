# **Project: Query Optimization and Indexing in a Lightweight DB Engine**

## **Background**

You will work with [pySimpleDB](https://github.com/CWSwastik/pySimpleDB). This is a lightweight educational database system that supports basic query execution but lacks:

* Effective query optimization
* Proper index structures

---

## **Objective**

Extend the system to improve query performance by implementing:

### **1. Join Order Optimization**

* Reorder cartesian products to reduce intermediate results
* Push selection conditions as early as possible

### **2. B-Tree Indexes**

* Implement a B-tree index
* Support insertion and search
* Integrate index usage into query execution

### **3. Composite Indexes**

* Support multi-attribute indexes

---

## **Schema**

### **Tables**

```sql
Student(
    s_id INT PRIMARY KEY,
    s_name VARCHAR(50),
    s_department VARCHAR(30),
    s_year INT
);

Instructor(
    i_id INT PRIMARY KEY,
    i_name VARCHAR(50),
    i_department VARCHAR(30)
);

Course(
    c_id INT PRIMARY KEY,
    c_title VARCHAR(100),
    c_department VARCHAR(30),
    c_credits INT
);

Section(
    sec_id INT PRIMARY KEY,
    sec_course_id INT,
    sec_instructor_id INT,
    sec_semester VARCHAR(10),
    sec_year INT,
    FOREIGN KEY (sec_course_id) REFERENCES Course(c_id),
    FOREIGN KEY (sec_instructor_id) REFERENCES Instructor(i_id)
);

Enrollment(
    e_id INT PRIMARY KEY,
    e_student_id INT,
    e_section_id INT,
    e_grade CHAR(2),
    FOREIGN KEY (e_student_id) REFERENCES Student(s_id),
    FOREIGN KEY (e_section_id) REFERENCES Section(sec_id)
);
```

---

## **Queries to Optimize**

### **Q1**

```sql
SELECT s_id, s_name
FROM Student, Enrollment, Section, Course
WHERE s_id = e_student_id
  AND e_section_id = sec_id
  AND sec_course_id = c_id
  AND c_department = 'CS';
```

---

### **Q2**

```sql
SELECT s_id, s_name
FROM Student, Enrollment
WHERE s_id = e_student_id
  AND e_grade = 'NC';
```

---

### **Q3**

```sql
SELECT i_id, i_name
FROM Instructor, Section
WHERE i_id = sec_instructor_id
  AND sec_semester = 'Fall'
  AND sec_year = 2024;
```

---

## **Execution Protocol**

Your implementation must run via the interface of pySimpleDB.

### **Run Commands**

```bash
python main.py --query Q1 --mode baseline
python main.py --query Q1 --mode opt
python main.py --query Q1 --mode index
python main.py --query Q1 --mode full
```

### **Modes Explained**

* **baseline**
  Runs the query without any optimization or indexing using the default execution plan.
  Serves as the reference for correctness and performance comparison.

* **opt**
  Runs the query with join reordering and selection pushdown enabled, but without indexes.
  Isolates the effect of query optimization alone.

* **index**
  Runs the query using indexes for selection and joins, but without changing join order.
  Isolates the benefit of indexing alone.

* **full**
  Runs the query with both optimization and index usage enabled.
  Expected to give the best performance.

---

## **Submission**

* Submit only the file named **`solution.py`** along with your report
* Package everything into a single zip file named:

```
Group_number_XXX.zip
```

### **Report Requirements**

Include:

* Design of the solution
* Optimizations implemented
* Observations

### **Performance Table**

Provide execution timings for:

| Query | baseline | opt | index | full |
| ----- | -------- | --- | ----- | ---- |
| Q1    |          |     |       |      |
| Q2    |          |     |       |      |
| Q3    |          |     |       |      |

---

## **Evaluation**

### **Criteria**

* Correctness and effectiveness on given queries
* Adherence to execution protocol
* Performance improvements

### **Hidden Tests**

* Evaluators may use:

  * Different schemas
  * Different queries

**Important Constraint:**

* Modify **only `solution.py`**
* Do **not** hardcode assumptions specific to the given schema

---

## **Duration**

* **Deadline:** April 22nd, 2026, 4:00 AM
* Demos will be scheduled later
* Work in groups of **minimum 6 members**

👉 Enter group details here:
[https://docs.google.com/spreadsheets/d/1A1GbWAyP0mwBDAa9rJHDVnmD7Wqq8OP0r5MKt7gWnPE/edit?usp=sharing](https://docs.google.com/spreadsheets/d/1A1GbWAyP0mwBDAa9rJHDVnmD7Wqq8OP0r5MKt7gWnPE/edit?usp=sharing)

---

## **Doubt Resolution**

Post queries on Piazza for shared benefit.

### **FD TAs**

* Swastik — [f20230043@hyderabad.bits-pilani.ac.in](mailto:f20230043@hyderabad.bits-pilani.ac.in)
* Siddharth — [f20230382@hyderabad.bits-pilani.ac.in](mailto:f20230382@hyderabad.bits-pilani.ac.in)
