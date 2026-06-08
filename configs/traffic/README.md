# Traffic Flow Profiles

Traffic flow profiles describe reproducible background traffic. Vehicles use
CARLA Traffic Manager; pedestrians use CARLA WalkerAIController. These actors
are background traffic:

- They are not ego vehicles.
- They are not scripted scenario actors such as lead/cut-in vehicles.
- They must use deterministic seeds.
- Background vehicles must produce `traffic_flow_contract` evidence.
- Background pedestrians must produce `pedestrian_flow_contract` evidence.

`traffic_flow_contract` passing only proves background traffic setup health. It
does not prove Apollo or Autoware natural-driving behavior.
`pedestrian_flow_contract` passing only proves WalkerAIController setup health.
It does not prove Apollo or Autoware pedestrian perception or avoidance.
