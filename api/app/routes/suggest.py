@app.route("/api/suggest")
def suggest():
    q = request.args.get("q", "")
    if len(q) < 2:
        return jsonify([])

    like_pattern = f"%{q}%"
    matches = (
        db.session.query(CityCounty.county)
        .filter(CityCounty.county.ilike(like_pattern))
        .distinct()
        .limit(5)
        .all()
    )
    return jsonify([m[0] for m in matches])
