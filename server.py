from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# Our in-memory database
books_db = {}
next_book_id = 1

@app.route('/', methods=['GET'])
def home():
    print("sind")
    # This tells Flask to look for a file named 'index.html' 
    # and send it to the user's browser.
    return render_template('./index.html')

@app.route('/books', methods=['POST'])
def create_book():
    global next_book_id
    
    # Get JSON data from the request
    data = request.get_json()
    print(data)
    
    # Create a new book object
    new_book = {
        'id': next_book_id,
        'title': data.get('title'),
        'author': data.get('author')
    }
    
    # Save to "database"
    books_db[next_book_id] = new_book
    next_book_id += 1
    
    # Return the created book and a 201 Created status code
    return jsonify(new_book), 201

@app.route('/books', methods=['GET'])
def get_all_books():
    # Convert our dictionary values to a list
    all_books = list(books_db.values())
    return jsonify(all_books), 200

@app.route('/books/<int:book_id>', methods=['GET'])
def get_book(book_id):
    book = books_db.get(book_id)
    if not book:
        return jsonify({'error': 'Book not found'}), 404
    
@app.route('/books/<int:book_id>', methods=['PUT'])
def update_book(book_id):
    book = books_db.get(book_id)
    if not book:
        return jsonify({'error': 'Book not found'}), 404
        
    data = request.get_json()
    # parses incoming JOSN data from the body of the HTTP request and converts it into a Python Object
    
    # Update the book's details
    book['title'] = data.get('title', book['title'])
    book['author'] = data.get('author', book['author'])
    
    return jsonify(book), 200
        
@app.route('/books/<int:book_id>', methods=['DELETE'])
def delete_book(book_id):
    if book_id in books_db:
        del books_db[book_id]
        return jsonify({'message': 'Book deleted successfully'}), 200
    
    return jsonify({'error': 'Book not found'}), 404

if __name__ == '__main__':
    app.run(debug=True)