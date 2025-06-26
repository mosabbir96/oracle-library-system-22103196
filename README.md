University Library Management System
Database (MySQL/Oracle/SQL Server) Course - Final Assignment

📋 Assignment Overview
This project is the final assignment for the Database course, implementing a complete University Library Management System using Oracle Database as per the assignment requirements.

Assignment Objectives
This assignment demonstrates my ability to:

📚 Design and implement a complete database system for library management

👥 Apply normalization principles and relational database concepts

🔄 Develop complex SQL queries and PL/SQL programming solutions

💰 Implement database security and performance optimization techniques

Learning Outcomes Achieved
Database Design: Applied ER modeling and normalization techniques

SQL Proficiency: Demonstrated mastery of DML, DDL, DCL, and TCL operations

PL/SQL Programming: Created procedures, functions, and triggers for business logic

Database Administration: Implemented user management and performance tuning

🎓 Assignment Parts Completed
Part 1: Database Design & Setup (15 Marks)
✅ Task 1.1: Created all required tables (BOOKS, MEMBERS, TRANSACTIONS) with proper constraints

✅ Task 1.2: Inserted sample data (20 books, 15 members, 25 transactions)

Part 2: Basic SQL Operations (20 Marks)
✅ Task 2.1: Data retrieval queries (available books, overdue members, top borrowed books, etc.)

✅ Task 2.2: Data manipulation (update fines, insert members, archive transactions, etc.)

Part 3: Advanced SQL Queries (25 Marks)
✅ Task 3.1: Join operations (INNER, LEFT, SELF, CROSS joins)

✅ Task 3.2: Subqueries (above average borrowing, member fines, etc.)

✅ Task 3.3: Aggregate & window functions (running totals, rankings, trends)

Part 4: PL/SQL Programming (25 Marks)
✅ Task 4.1: ISSUE_BOOK stored procedure

✅ Task 4.2: CALCULATE_FINE function

✅ Task 4.3: UPDATE trigger for book returns

Part 5: Database Administration (15 Marks)
✅ Task 5.1: User management (librarian and student_user roles)

✅ Task 5.2: Performance optimization (indexing and execution plans)

🛠️ Technologies Used
Component

Technology

Database

Oracle Database

Query Language

SQL (DML, DDL, DCL, TCL)

Procedural Language

PL/SQL

Administration

Oracle Database Tools

⭐ Key Features
1. Comprehensive Data Management
📖 Books Management
Store complete book details: title, author, publisher, ISBN

Category classification and inventory tracking

Available copies monitoring

👤 Member Management
Support for multiple user types: students, faculty, staff

Complete contact information storage

Membership type and status tracking

📝 Transaction Management
Complete borrow/return operation tracking

Issue date, return date, and due date management

Fine calculation and status monitoring

2. Advanced Querying Capabilities
Data Retrieval Operations
Real-time book availability checking

Overdue items identification

Most borrowed books analytics

Member activity reports

Data Manipulation
Automated fine updates

New member registration

Historical data archiving

Join Operations
INNER JOIN - Active member transactions

LEFT JOIN - All books with borrowing history

SELF JOIN - Member referral systems

CROSS JOIN - Comprehensive reporting matrices

Subquery Operations
Most active member identification

High-fine user detection

Least borrowed books analysis

Advanced filtering and ranking

Analytics & Reporting
Aggregate Functions: SUM, COUNT, AVG for statistics

Window Functions: RANK(), NTILE(), LAG() for trends

Statistical Analysis: Usage patterns and borrowing trends

3. PL/SQL Automation & Business Logic
📚 ISSUE_BOOK Procedure
-- Automated book issuing with validation

Checks real-time book availability

Creates transaction records

Updates inventory automatically

Comprehensive error handling

💰 CALCULATE_FINE Function
-- Dynamic fine calculation

Accepts transaction ID parameter

Calculates fines based on delay and daily rates

Returns precise fine amounts

🔄 UPDATE_AVAILABLE_COPIES Trigger
-- Automatic inventory management

Fires on book return operations

Updates available_copies in real-time

Maintains data consistency

4. Database Administration
🔐 Security & User Management
Role-Based Access Control

librarian role: Full system access

student_user role: Limited read access

Privilege management and security policies

⚡ Performance Optimization
Strategic indexing on high-frequency columns

Query execution plan analysis

Performance tuning recommendations

Database monitoring and optimization

📁 Assignment Submission Structure
oracle-library-system-[student-id]/
│
├── README.md             → This documentation file
│
└── sql/
├── setup.sql         → Part 1: DDL, table creation, sample data
├── queries.sql       → Part 2 & 3: All required queries with comments
├── plsql.sql         → Part 3: Procedures, functions, triggers
└── admin.sql         → Part 4: User management & performance tuning

File Descriptions
setup.sql
Database schema creation (DDL statements)

Table definitions with all constraints

Sample data insertion for testing

Primary and foreign key relationships

queries.sql
All assignment queries organized by requirements

Detailed comments explaining each query's purpose

Expected output descriptions

Performance considerations noted

plsql.sql
Complete PL/SQL code blocks

Stored procedures with parameter documentation

Functions with return value specifications

Triggers with before/after logic explanations

admin.sql
User role creation and privilege assignments

Index creation for performance optimization

Database administration commands

Security implementation details

🚀 How to Run
Connect to Oracle Database

sqlplus username/password@database

Execute files in order:

@sql/setup.sql    -- Creates tables and inserts sample data
@sql/queries.sql  -- Runs all required queries
@sql/plsql.sql    -- Creates procedures, functions, triggers
@sql/admin.sql    -- Sets up users and indexing

📊 Database Schema
Tables Created
BOOKS - Book inventory with book_id, title, author, publisher, publication_year, isbn, category, total_copies, available_copies, price

MEMBERS - Member information with member_id, first_name, last_name, email, phone, address, membership_date, membership_type

TRANSACTIONS - Transaction records with transaction_id, member_id, book_id, issue_date, due_date, return_date, fine_amount, status

📝 Assignment Requirements
Based on the assignment document, this implementation includes:

Sample Data Requirements
20 books across different categories

15 members (students, faculty, staff)

25 transactions (returned, pending, overdue)

Key Queries Implemented
Books with available copies less than total copies

Members with overdue books

Top 5 most borrowed books

Members who never returned books on time

Fine calculations (₹5 per day after due date)

PL/SQL Components
ISSUE_BOOK procedure with member_id and book_id parameters

CALCULATE_FINE function returning ₹5 per overdue day

Trigger to update available_copies on book return

User Management
librarian user: Full access to all tables

student_user: SELECT only on BOOKS table
