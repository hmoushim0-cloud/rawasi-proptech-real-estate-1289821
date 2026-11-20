package sa.factory.rawasiproptechreal.data.repository

import sa.factory.rawasiproptechreal.domain.repository.RawasiProptechRepository
import sa.factory.rawasiproptechreal.hardware.HardwareSensorDriver
import sa.factory.rawasiproptechreal.model.RawasiProptechItem
import sa.factory.rawasiproptechreal.model.RawasiProptechOrder
import sa.factory.rawasiproptechreal.model.RawasiProptechTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class RawasiProptechRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : RawasiProptechRepository {

    private val _catalog = MutableStateFlow<List<RawasiProptechItem>>(listOf(
        RawasiProptechItem("1", "مسح السحابة النقطية بالليزر ثلاثي الأبعاد", "التقاط دقيق للأبعاد الهندسية عبر جهاز 360 LiDAR Spatial Scanner لتوليد شبكة مضلعات بدقة ملليمترية", 4500.0, "دقة 99.8%", "المسح المكاني بالليزر"),
        RawasiProptechItem("2", "مزامنة جولات ماتربرت الفضائية", "استيراد بيانات Matterport Pro3 لإنشاء جولات افتراضية تفاعلية للمساحات السكنية بأسلوب 5 نجوم", 3200.0, "تزامن فوري", "النمذجة المعمارية ثلاثية الأبعاد"),
        RawasiProptechItem("3", "محاكاة إضاءة في-راي والمواد الواقعية", "تطبيق خامات الذهب والرخام الإيطالي مع حسابات تشتت الضوء الواقعي عبر محرك سكتش أب", 6800.0, "معتمد", "إضاءة في-راي والرندرة الواقعية")
    ))
    override val catalogFlow: Flow<List<RawasiProptechItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<RawasiProptechItem>>(emptyList())
    override val activeCart: StateFlow<List<RawasiProptechItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<RawasiProptechTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            RawasiProptechTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: RawasiProptechItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: RawasiProptechItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): RawasiProptechOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = RawasiProptechOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
