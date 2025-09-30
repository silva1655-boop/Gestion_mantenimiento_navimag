"""
Maintenance Management Program for Fleet of Trucks

This script defines classes and functions to manage maintenance operations for a fleet
of vehicles.  It models equipment, components, maintenance plans, work orders,
inventory of spare parts, and provides a simple scheduling engine that triggers
maintenance when hours, distance or time thresholds are reached.  The design
is intentionally simplified to run without external dependencies and is meant
to serve as a starting point for a more comprehensive web application.

Key features implemented:

* Equipment and component definitions with maintenance intervals.
* Tracking of hour‑meter (horómetro) and odometer readings for each piece of
  equipment.
* Automatic generation of work orders when maintenance is due based on
  hours, kilometres or calendar days.
* Inventory management with stock tracking and association of spare parts to
  components.
* Recording of completed maintenance and updating last service data.
* Calculation of simple reliability metrics such as MTBF (Mean Time Between
  Failures) and MTTR (Mean Time To Repair) for each component.

Note: This program does not include a graphical user interface.  It uses
command‑line interaction in the example at the bottom.  For production use
you would typically integrate these classes with a database and a web or
mobile front end.  The logic here, however, demonstrates how to structure
the core of a maintenance management system in Python.
"""

from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class Component:
    """Represents a component (e.g., suspension, brakes) with maintenance
    intervals and criticidad (criticality).

    Attributes
    ----------
    name: str
        Component name (e.g. "Amortiguador delantero").
    criticidad: str
        Level of criticidad: "alta", "media" or "baja".
    hours_interval: Optional[int]
        Number of running hours between preventive services.  None if not
        defined.
    km_interval: Optional[int]
        Number of kilometres between services.  None if not defined.
    days_interval: Optional[int]
        Number of days between services.  None if not defined.
    """

    name: str
    criticidad: str = "media"
    hours_interval: Optional[int] = None
    km_interval: Optional[int] = None
    days_interval: Optional[int] = None


@dataclass
class ComponentRecord:
    """Tracks the last service information for a component on a specific
    equipment.  This allows the scheduler to determine when the next service
    is due.
    """

    component: Component
    last_service_date: datetime.date
    last_service_hours: float
    last_service_km: float

    def is_due(self, current_date: datetime.date, current_hours: float, current_km: float) -> Tuple[bool, str]:
        """Determine whether this component is due for maintenance.

        Parameters
        ----------
        current_date: datetime.date
            The date at which to evaluate the maintenance requirement.
        current_hours: float
            The current accumulated running hours for the equipment.
        current_km: float
            The current accumulated distance for the equipment.

        Returns
        -------
        due: bool
            True if maintenance is due based on any of the defined intervals.
        reason: str
            Human readable string explaining which interval triggered the due.
        """
        # Check hours interval
        if self.component.hours_interval is not None:
            delta_hours = current_hours - self.last_service_hours
            if delta_hours >= self.component.hours_interval:
                return True, f"Horas alcanzadas: {delta_hours:.0f}h ≥ {self.component.hours_interval}h"

        # Check kilometres interval
        if self.component.km_interval is not None:
            delta_km = current_km - self.last_service_km
            if delta_km >= self.component.km_interval:
                return True, f"Kilómetros alcanzados: {delta_km:.0f} km ≥ {self.component.km_interval} km"

        # Check days interval
        if self.component.days_interval is not None:
            delta_days = (current_date - self.last_service_date).days
            if delta_days >= self.component.days_interval:
                return True, f"Días alcanzados: {delta_days} días ≥ {self.component.days_interval} días"

        return False, ""


@dataclass
class Equipment:
    """Represents a single piece of equipment (truck) in the fleet.
    It tracks the horometro and odometro readings and the maintenance records
    for each installed component.
    """

    id: str
    description: str
    horometro: float = 0.0
    odometro: float = 0.0
    status: str = "operativo"
    components: Dict[str, ComponentRecord] = field(default_factory=dict)

    def register_component(self, component: Component, service_date: Optional[datetime.date] = None,
                           service_hours: Optional[float] = None, service_km: Optional[float] = None) -> None:
        """Associate a component with this equipment and initialise its last service info.

        If no service information is provided, we assume the component is new and the
        last service date is today and hours/kilometres equal the current readings.
        """
        service_date = service_date or datetime.date.today()
        service_hours = service_hours if service_hours is not None else self.horometro
        service_km = service_km if service_km is not None else self.odometro
        self.components[component.name] = ComponentRecord(component, service_date, service_hours, service_km)

    def update_horometro(self, additional_hours: float) -> None:
        """Add hours to the horometro reading."""
        self.horometro += additional_hours

    def update_odometro(self, additional_km: float) -> None:
        """Add kilometres to the odometro reading."""
        self.odometro += additional_km

    def set_status(self, new_status: str) -> None:
        """Update the operational status of the equipment."""
        self.status = new_status


@dataclass
class WorkOrder:
    """Represents a maintenance work order.

    Attributes
    ----------
    id: str
        Unique identifier for the work order.
    equipment_id: str
        ID of the equipment on which the maintenance will be performed.
    component_name: str
        Name of the component requiring service.
    due_date: datetime.date
        Date by which the work should be executed.
    reason: str
        Explanation of why the service is due.
    status: str
        Current status: "pendiente", "en progreso", "completada".
    created_at: datetime.datetime
        Timestamp when the order was created.
    completed_at: Optional[datetime.datetime]
        Timestamp when the order was completed.
    """

    equipment_id: str
    component_name: str
    due_date: datetime.date
    reason: str
    status: str = "pendiente"
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: datetime.datetime = field(default_factory=lambda: datetime.datetime.now())
    completed_at: Optional[datetime.datetime] = None

    def mark_completed(self, completion_time: Optional[datetime.datetime] = None) -> None:
        """Mark the work order as completed and record the completion timestamp."""
        self.status = "completada"
        self.completed_at = completion_time or datetime.datetime.now()


class Inventory:
    """Simple inventory management for spare parts.

    Maintains stock levels and allows reservation of parts for work orders.  In a
    more comprehensive system you could track batches, serial numbers, vendors,
    and automatically trigger purchase orders when stock falls below minimum.
    """

    def __init__(self) -> None:
        # Maps part name/code to (current stock, minimum stock)
        self._stock: Dict[str, Tuple[int, int]] = {}

        # Mapping part name to the component names it fits
        self._part_mapping: Dict[str, List[str]] = {}

    def add_part(self, part_name: str, initial_stock: int, min_stock: int, fits_components: List[str]) -> None:
        self._stock[part_name] = (initial_stock, min_stock)
        self._part_mapping[part_name] = fits_components

    def reserve_part(self, part_name: str, quantity: int = 1) -> bool:
        """Reserve a part from the inventory if available.

        Returns True if the reservation succeeded, False if there is not enough stock.
        """
        current, min_stock = self._stock.get(part_name, (0, 0))
        if current >= quantity:
            self._stock[part_name] = (current - quantity, min_stock)
            return True
        return False

    def get_stock(self, part_name: str) -> int:
        return self._stock.get(part_name, (0, 0))[0]

    def parts_for_component(self, component_name: str) -> List[str]:
        """Return a list of part names that can be used for the given component."""
        return [p for p, comps in self._part_mapping.items() if component_name in comps]

    def low_stock_alerts(self) -> List[str]:
        """Return a list of parts whose stock is at or below the minimum."""
        alerts = []
        for part, (qty, min_qty) in self._stock.items():
            if qty <= min_qty:
                alerts.append(part)
        return alerts


class Scheduler:
    """Maintenance scheduler that monitors equipment and generates work orders when
    service intervals are reached.

    The scheduler does not actively run on its own thread; instead, you call
    `check_due_maintenance` periodically (e.g., daily) to produce any due
    work orders.  In a production system this could be invoked by a cron job
    or a background task.
    """

    def __init__(self, equipment_registry: Dict[str, Equipment], inventory: Inventory) -> None:
        self.equipment_registry = equipment_registry
        self.inventory = inventory
        self.pending_orders: List[WorkOrder] = []

    def check_due_maintenance(self, reference_date: Optional[datetime.date] = None) -> List[WorkOrder]:
        """Evaluate all equipment and components to see if maintenance is due.

        Parameters
        ----------
        reference_date: datetime.date, optional
            The date at which to check for maintenance due.  Defaults to today.

        Returns
        -------
        due_orders: list of WorkOrder
            List of work orders generated because maintenance is due.
        """
        reference_date = reference_date or datetime.date.today()
        due_orders: List[WorkOrder] = []

        for eq in self.equipment_registry.values():
            # Skip equipment not in service
            if eq.status not in ("operativo", "disponible", "en servicio"):
                continue
            for record in eq.components.values():
                due, reason = record.is_due(reference_date, eq.horometro, eq.odometro)
                if due:
                    # Create a work order if one isn't already pending
                    if not any(order for order in self.pending_orders
                               if order.equipment_id == eq.id and order.component_name == record.component.name and order.status == "pendiente"):
                        order = WorkOrder(eq.id, record.component.name, reference_date, reason)
                        due_orders.append(order)
                        self.pending_orders.append(order)
        return due_orders

    def complete_order(self, order_id: str) -> None:
        """Mark the specified work order as completed and update last service data.

        When an order is completed, update the last service date, hours and kilometres
        for the corresponding component on the equipment.
        """
        for order in self.pending_orders:
            if order.id == order_id:
                order.mark_completed()
                # Update last service information
                equipment = self.equipment_registry[order.equipment_id]
                record = equipment.components[order.component_name]
                record.last_service_date = order.completed_at.date()
                record.last_service_hours = equipment.horometro
                record.last_service_km = equipment.odometro
                return


class FailureLog:
    """Records failures reported for equipment and components.

    Keeping a separate log of failures helps analyse trends and calculate metrics
    such as MTBF and MTTR.
    """

    def __init__(self) -> None:
        # Each entry is a tuple (timestamp, equipment_id, component_name, description, repair_time_hours)
        self.entries: List[Tuple[datetime.datetime, str, str, str, float]] = []

    def log_failure(self, equipment_id: str, component_name: str, description: str, repair_time_hours: float) -> None:
        self.entries.append((datetime.datetime.now(), equipment_id, component_name, description, repair_time_hours))

    def calculate_mtbf(self, equipment_id: str, component_name: str) -> Optional[float]:
        """Calculate mean time between failures (MTBF) for a component on a specific equipment.

        Returns MTBF in hours if at least two failures exist; otherwise returns None.
        """
        # Filter entries for this equipment/component
        times = [entry[0] for entry in self.entries if entry[1] == equipment_id and entry[2] == component_name]
        if len(times) < 2:
            return None
        times.sort()
        total_interval = sum(((times[i] - times[i - 1]).total_seconds() / 3600) for i in range(1, len(times)))
        return total_interval / (len(times) - 1)

    def calculate_mttr(self, equipment_id: str, component_name: str) -> Optional[float]:
        """Calculate mean time to repair (MTTR) based on failure logs.

        Returns MTTR in hours if failures exist; otherwise returns None.
        """
        repair_times = [entry[4] for entry in self.entries if entry[1] == equipment_id and entry[2] == component_name]
        if not repair_times:
            return None
        return sum(repair_times) / len(repair_times)


def example_usage() -> None:
    """Demonstrates how to use the maintenance system.

    This function builds a minimal dataset and runs through updating
    horometro readings, scheduling maintenance, completing work orders and
    logging failures.  The example runs in a single pass but in practice
    you would invoke the scheduler regularly (e.g. daily) and interact with
    the system via a user interface.
    """
    # Create inventory and define some spare parts
    inventory = Inventory()
    inventory.add_part("Amortiguador delantero", initial_stock=10, min_stock=2, fits_components=["Amortiguadores"])
    inventory.add_part("Plumillas limpiaparabrisas", initial_stock=20, min_stock=5, fits_components=["Limpiaparabrisas"])
    inventory.add_part("Foco delantero", initial_stock=30, min_stock=5, fits_components=["Luces"])

    # Define components
    susp = Component("Amortiguadores", "alta", hours_interval=500, km_interval=50000, days_interval=365)
    wiper = Component("Limpiaparabrisas", "alta", hours_interval=200, days_interval=180)
    lights = Component("Luces", "alta", days_interval=90)

    # Create equipment
    fleet: Dict[str, Equipment] = {}
    eq1 = Equipment("TR-001", "Tracto Kalmar")
    eq2 = Equipment("TR-002", "Tracto Terberg")

    # Register components and initial service data
    for eq in (eq1, eq2):
        eq.register_component(susp)
        eq.register_component(wiper)
        eq.register_component(lights)

    fleet[eq1.id] = eq1
    fleet[eq2.id] = eq2

    # Create scheduler
    scheduler = Scheduler(fleet, inventory)

    # Simulate operation: advance horometro and odometro
    eq1.update_horometro(550)
    eq1.update_odometro(52000)
    eq2.update_horometro(190)
    eq2.update_odometro(12000)

    # Check maintenance due
    due_orders = scheduler.check_due_maintenance()
    print("\nOrdenes generadas:")
    for order in due_orders:
        print(f"Equipo: {order.equipment_id} | Componente: {order.component_name} | Motivo: {order.reason}")

    # Suppose we complete the first order and consume a spare part
    if due_orders:
        first_order = due_orders[0]
        # Reserve the appropriate spare part for the component (if available)
        possible_parts = inventory.parts_for_component(first_order.component_name)
        if possible_parts:
            part_name = possible_parts[0]
            if inventory.reserve_part(part_name):
                print(f"Repuesto '{part_name}' reservado para la orden {first_order.id}")
            else:
                print(f"No hay stock suficiente del repuesto '{part_name}'")
        scheduler.complete_order(first_order.id)
        print(f"Orden {first_order.id} completada para equipo {first_order.equipment_id}")

    # Check inventory alerts
    low_parts = inventory.low_stock_alerts()
    if low_parts:
        print("\nPartes en nivel mínimo:")
        for p in low_parts:
            print(f" - {p} (stock actual: {inventory.get_stock(p)})")

    # Log a failure and calculate metrics
    failure_log = FailureLog()
    failure_log.log_failure(eq1.id, "Amortiguadores", "Falla de amortiguador", repair_time_hours=3.5)
    failure_log.log_failure(eq1.id, "Amortiguadores", "Otra falla", repair_time_hours=2.0)
    mtbf = failure_log.calculate_mtbf(eq1.id, "Amortiguadores")
    mttr = failure_log.calculate_mttr(eq1.id, "Amortiguadores")
    print("\nMétricas para el equipo TR-001 (Amortiguadores):")
    print(f"MTBF: {'{:.1f}'.format(mtbf) if mtbf else 'N/A'} horas")
    print(f"MTTR: {'{:.1f}'.format(mttr) if mttr else 'N/A'} horas")


if __name__ == "__main__":
    example_usage()