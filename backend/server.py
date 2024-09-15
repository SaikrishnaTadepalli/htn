from flask import Flask, request, jsonify

from graphdbconnector import GraphDBConnector

app = Flask(__name__)

@app.route('/create-user', methods=['POST'])
def create_user():
    """
    Creates a new user:
    - Make sure user's 'email' property is unique
    """
    try:
        data = request.get_json()

        email = data.get('email')
        firstname = data.get('firstname')
        lastname = data.get('lastname')

        if not email or not firstname or not lastname:
            return jsonify({'error': 'Missing required fields'}), 400

        try:
            GraphDBConnector.add_user(
                email, firstname, lastname
            )
        except:
            return jsonify({'error': 'Email already exists'}), 400

        # Return a success response
        return jsonify({'message': 'User created successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/submit-user-survery', methods=['POST'])
def submit_user_survery():
    """
    Connects the User to the non-user nodes
    """
    pass

@app.route('/login', methods=['POST'])
def login():
    """
    Succeeds if user with the inputted email exists, else fails
    """
    try:
        data = request.get_json()
        
        email = data.get('email')

        if not email:
            return jsonify({'error': 'Missing required fields'}), 400

        return jsonify({
            'message': GraphDBConnector.is_user(email)
        }), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get-global-connections', methods=['GET'])
def get_global_connections():
    try:
        data = request.get_json()
        
        email = data.get('email')

        if not email:
            return jsonify({'error': 'Missing required fields'}), 400

        return jsonify({
            'message': GraphDBConnector.find_global_connections(email)
        }), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/get-date-user', methods=['GET'])
def get_date_user():
    try:
        data = request.get_json()
        email = data.get('email')
        if not email:
            return jsonify({'error': 'Missing required fields'}), 400
        
        date_user_email = GraphDBConnector.find_date(email)

        if date_user_email == "":
            return jsonify({'error': 'Unable to find a date at this time'}), 400

        return jsonify({'message': date_user_email}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500