# ##Set up venv
#
# python3 -m venv venv
# virtualenv venv
# source venv/bin/activate
# pip3 install flask_marshmallow 
# pip3 install flask_sqlalchemy
# pip3 install marshmallow-sqlalchemy
# pip3 install flask-smorest
# python3 carApp.py

# ##Leave venv
#
# deactivate

# #####################
# Create a RESTful API to interact with the dataset. Make sure to
# include the following functionality:

# Standard CRUD operations on a particular data point
# Ability to fetch aggregate data on a dimension of your choice.
# Ideas are: number of vehicles purchased per store,
# Filtering based on price and store location average purchase
# price per store, sorted list of most commonly sold makes/brands, etc.
# Must use a SQL-based database, we recommend SQLite to minimize
# development overhead

# A few notes:
# Focus on how you'd design a schema that enables evolution over 
# time with minimal overhead. Keep performance considerations in 
# mind when designing indexes, and make sure to document
# the trade-offs that your decisions will entail

# ########## Data dictionary
#
# Column Title             Column Description
# VIN                    Vehicle Identification Number (Unique vehicle ID)
# CarYear                Model year of vehicle
# Color                    Color of vehicle
# VehBody                Body type
# EngineType                Engine size in cylinders
# Make                    Model name
# Miles                    Odometer reading at time of inventory
# SaleType                (R) for resale, (N) for new vehicle
# Odometer                Accuracy of odometer reading
# VehType                Passenger, Truck, or Motorcycle
# LocationNum            ID number of store that acquired vehicle
# CarType                Vehicle segment type
# EngineLiters            Engine displacement in Liters
# FuelType                Fuel type of vehicle
# Transmission            Transmission type of vehicle
# SaleLoc                ID number of dealership that sold vehicle
# PurchVal                Purchase price of vehicle

from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import os
from sqlalchemy import func
from sqlalchemy import *

from flask_smorest import Api, Blueprint, abort
from webargs import fields
from webargs.flaskparser import use_args

from flask import Flask, jsonify, request
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))

# Smorest
app.config['OPENAPI_VERSION'] = '3.0.2'
app.config['OPENAPI_URL_PREFIX'] = "swagger"

# database setup
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db.sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# nitialize
db = SQLAlchemy(app)
ma = Marshmallow(app)
api = Api(app)


# db table
class car(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    VIN = db.Column(db.String)
    CarYear = db.Column(db.Integer)
    Color = db.Column(db.String)
    VehBody = db.Column(db.String)
    EngineType = db.Column(db.String)
    Make = db.Column(db.String)
    Miles = db.Column(db.Float)
    Odometer = db.Column(db.String)
    Brand = db.Column(db.String)
    LocationNum = db.Column(db.Integer)
    CarType = db.Column(db.String)
    EngineLiters = db.Column(db.String)
    FuelType = db.Column(db.String)
    Transmission = db.Column(db.String)
    SaleLoc = db.Column(db.Integer)
    PurchVal = db.Column(db.Float)

    def __repr__(self):
        return "<Car " + self.name + ">"


# db.create_all()
# db.session.commit()

# Car schema
class carSchema(ma.Schema):
    class Meta:
        fields = (
            "id",
            "VIN",
            "CarYear",
            "Color",
            "VehBody",
            "EngineType",
            "Make",
            "Miles",
            "Odometer",
            "Brand",
            "VehType",
            "LocationNum",
            "CarType",
            "EngineLiters",
            "FuelType",
            "Transmission",
            "SaleLoc",
            "PurchVal")


# car sum schema
class carSumSchema(ma.Schema):
    class Meta:
        fields = ("total", "sum")


# car location number schema
class carLocoSchema(ma.Schema):
    class Meta:

        fields = ("count", "LocationNum")

# car make schema
class carMakeSchema(ma.Schema):
    class Meta:

        fields = ("count", "Make")

# init schema
car_schema = carSchema()
cars_schema = carSchema(many=True)
car_sum_schema = carSumSchema()
car_LocationNum = carLocoSchema(many=True)
car_make = carMakeSchema(many=True)

blp = Blueprint(
    'cars', 'cars', url_prefix='/car',
    description='Operations on cars'
)

# get all cars
@blp.route('', methods=['GET'])
def getCars():
    allCars = car.query.all()
    # print(allCars)
    result = cars_schema.dump(allCars)
    # print(result)
    return jsonify(result)

# get a single car
@blp.route('/<id>', methods=['GET'])
def getCar(id):
    onecar = car.query.get(id)
    return car_schema.jsonify(onecar)

# Get odometer for one car
@blp.route('/<id>/Odometer', methods=['GET'])
def getOdo(id):
    carId = car.query.get(id)
    odometer = carId.Odometer
    # print(carId, odometer)
    return jsonify(odometer)


# Calculate the average purchase value of all cars
@blp.route('/PurchVal', methods=['GET'])
def carAvg():
    totalVal = db.session.query(func.avg(car.PurchVal)).scalar()
    totalCount = db.session.query(func.count(car.PurchVal)).scalar()
    print("Average PurchVal = ", totalVal)
    print("totalVal is a type ", type(totalVal))
    # print(func.now())
    print(car_schema.jsonify(totalVal))
    # return jsonify({'sum': str(totalVal)})
    return car_sum_schema.jsonify({'sum': str(totalVal), 'total': str(totalCount)})

# Calculate the number of vehicles purchased per store
# SELECT COUNT(*), LocationNum
#  FROM car group by LocationNum;

@blp.route('/LocationNum', methods=['GET'])
@use_args({
    "LocationNum": fields.Str(required=False), 
    "CountAtLeast": fields.Int(required=False, missing=100)})
def carLocation(args):
    minimumCount = int(args["CountAtLeast"])

    loco = db.session.query(func.count(car.LocationNum).label("count"),
        car.LocationNum.label("LocationNum")).\
        group_by(car.LocationNum).\
        having(func.count(car.LocationNum) > int(minimumCount))

    # print(loco,loco.all())
    return car_LocationNum.jsonify(loco.all())

#calculate sorted list of the following: 
#most commonly sold makes/brand/SaleType for total and per SaleLoc
#SELECT COUNT(*), Make, Brand, SaleType
#FROM car group by Make
#order by COUNT(*) desc;

@blp.route('/Make', methods=['GET'])
@use_args({"Make": fields.Str(required=False),"CountAtLeast": fields.Int(required=False, missing=100)})
def commonlySold(args):
    minimumCount = int(args["CountAtLeast"])
    carMake = db.session.query(func.count(car.Make).label("count"),car.Make.label("Make")).\
    group_by(car.Make).\
    having(func.count(car.Make) > int(minimumCount))

    # print(loco,loco.all())
    return car_make.jsonify(carMake.all())


api.register_blueprint(blp)

# run server
if __name__ == "__main__":
    app.run(debug=True, port=5000)
