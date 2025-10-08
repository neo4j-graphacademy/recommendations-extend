from united_states import UnitedStates
from faker import Faker

def generate_coord_by_state(seed, state):
    """
    Generate a random GPS coordinate within the bounding box of the given state.
    Ensures that the generated coordinate corresponds to a location within the state.

    Bit hacky, bit slow, but works well enough for generating sample data.
    """

    fake = Faker(seed)
    us = UnitedStates()

    state_info = us.by_abbr[state]

    lats = [state_info.bbox.bottom, state_info.bbox.top]
    longs = [state_info.bbox.left, state_info.bbox.right]

    if state == "HI":
        # Hawaii is made up of islands spread over a large area, so we use a smaller bounding box
        lats = [18.910361, 20.3]
        longs = [-156.5, -154.5]

    found = False
    i = 0
    while not found:
        
        lat = float(fake.pydecimal(right_digits=6, min_value=lats[0], max_value=lats[1]))
        long = float(fake.pydecimal(right_digits=6, min_value=longs[0], max_value=longs[1]))
        
        # check that the generated coordinate is actually in the state
        if len(us.from_coords(float(lat), float(long))) > 0:
            found = True
        
        i += 1
        if i > 10:
            print("could not find in coord in", state)
            break

    return (lat, long)

if __name__ == "__main__":
    for i in range(10):
        print(generate_coord_by_state(123456, "FL"))