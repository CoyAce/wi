package wi

import (
	"encoding/hex"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestSignMarshal(t *testing.T) {
	sign := "default"
	uuid := "mock#00001"
	s := SignReq{0, SignBody{Sign: sign, UUID: uuid}}
	pkt, _ := s.Marshal()
	t.Logf("pkt: [%v]", hex.EncodeToString(pkt))
}

func TestMsgMarshal(t *testing.T) {
	sign := "default"
	uuid := "mock#00001"
	s := SignReq{0, SignBody{Sign: sign, UUID: uuid}}
	msg := SignedMessage{s, time.Now().UnixMilli(), []byte("hello")}
	pkt, _ := msg.Marshal()
	t.Logf("pkt: [%v]", hex.EncodeToString(pkt))
}

func TestOpCode(t *testing.T) {
	s := OpSign
	op := OpSignOut
	pkt, err := op.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if s.Unmarshal(pkt) == nil {
		t.Errorf("expected op sign out; actual %#v", s)
	}
	if op.Unmarshal(pkt) != nil {
		t.Errorf("expected op sign out; actual %#v", op)
	}
}

func TestRangeTruncate(t *testing.T) {
	n := Nck{FileId: 1, ranges: []Range{
		{141889, 141907}, {141909, 141932}, {141934, 141935}, {141937, 141941}, {141943, 141947}, {141949, 141966}, {141968, 141972}, {141974, 141976}, {141978, 142015}, {142017, 142024}, {142026, 142049}, {142051, 142097}, {142099, 142112}, {142114, 142124}, {142126, 142142}, {142144, 142147}, {142149, 142153}, {142155, 142202}, {142204, 142206}, {142208, 142211}, {142213, 142219}, {142221, 142221}, {142223, 142226}, {142228, 142249}, {142251, 142257}, {142259, 142277}, {142279, 142300}, {142302, 142329}, {142331, 142344}, {142346, 142366}, {142368, 142389}, {142391, 142404}, {142532, 142532}, {142660, 142660}, {142682, 142689}, {142691, 142692}, {142694, 142696}, {142698, 142699}, {142701, 142707}, {142709, 142720}, {142788, 142788}, {142916, 142929}, {142931, 142939}, {142941, 142949}, {142951, 142952}, {142954, 142964}, {142966, 142968}, {142970, 143009}, {143011, 143019}, {143021, 143022}, {143024, 143024}, {143026, 143047}, {143049, 143053}, {143055, 143060}, {143062, 143074}, {143076, 143085}, {143087, 143094}, {143096, 143112}, {143114, 143116}, {143118, 143118}, {143120, 143125},
		{143127, 143139}, {143141, 143145}, {143148, 143148}, {143150, 143153}, {143155, 143177}, {143179, 143181}, {143309, 143309}, {143437, 143437}, {143565, 143565}, {143693, 143693}, {143821, 143851}, {143853, 143866}, {143868, 143883}, {143885, 143892}, {143894, 143915}, {143917, 143923}, {143925, 143926}, {143928, 143943}, {143945, 143951}, {143953, 143955}, {143957, 143963}, {143965, 143970}, {143972, 143976}, {143978, 143981}, {143983, 143983}, {143985, 143985}, {143987, 144034}, {144036, 144044}, {144046, 144046}, {144048, 144059}, {144061, 144080}, {144082, 144093}, {144095, 144146}, {144148, 144161}, {144164, 144200}, {144202, 144205}, {144207, 144213}, {144215, 144264}, {144266, 144305}, {144307, 144353}, {144356, 144360}, {144362, 144422}, {144549, 144551}, {144553, 144554}, {144556, 144578}, {144708, 144708}, {144834, 144834}, {144960, 144960}, {144963, 145011}, {145013, 145034}, {145036, 145049}, {145051, 145055}, {145057, 145069}, {145071, 145084}, {145086, 145104}, {145106, 145158}, {145160, 145179}, {145181, 145191}, {145193, 145219}, {145221, 145237}, {145239, 145255}, {145257, 145282}, {145284, 145313}, {145315, 145315},
		{145317, 145319}, {145321, 145332}, {145334, 145335}, {145337, 145339}, {145341, 145349}, {145351, 145362}, {145364, 145376}, {145378, 145399}, {145401, 145405}, {145407, 145410}, {145412, 145424}, {145426, 145433}, {145435, 145439}, {145441, 145453}, {145455, 145455}, {145457, 145464}, {145466, 145481}, {145483, 145485}, {145487, 145516}, {145644, 145644}, {145772, 145772}, {145903, 145903}, {146028, 146028}, {146093, 146098}, {146130, 146130}, {146132, 146133}, {146150, 146151}, {146164, 146164}, {146168, 146182}, {146184, 146189}, {146191, 146199}, {146201, 146257}, {146259, 146263}, {146265, 146265}, {146267, 146284}, {146286, 146289}, {146291, 146312}, {146314, 146341}, {146407, 146407}, {146409, 146411}, {146444, 146445}, {146464, 146464}, {146481, 146482}, {146484, 146509}, {146511, 146520}, {146522, 146526}, {146528, 146530}, {146532, 146543}, {146545, 146549}, {146551, 146554}, {146556, 146564}, {146629, 146632}, {146634, 146634}, {146667, 146668}, {146670, 146670}, {146694, 146694}, {146701, 146701}, {146703, 146712}, {146714, 146731}, {146733, 146738}, {146740, 146740}, {146804, 146804}, {146807, 146809}, {146843, 146844},
		{146861, 146861}, {146871, 146871}, {146877, 146877}, {146879, 146908}, {146910, 146916}, {146918, 146947}, {146949, 146953}, {146955, 146962}, {146964, 147003}, {147068, 147073}, {147106, 147107}, {147126, 147126}, {147139, 147139}, {147142, 147142}, {147144, 147152}, {147154, 147154}, {147156, 147162}, {147164, 147167}, {147169, 147174}, {147176, 147227}, {147229, 147229}, {147231, 147232}, {147234, 147237}, {147239, 147263}, {147265, 147272}, {147274, 147291}, {147293, 147295}, {147297, 147301}, {147303, 147319}, {147321, 147328}, {147330, 147339}, {147342, 147352}, {147354, 147366}, {147368, 147382}, {147384, 147395}, {147397, 147412}, {147414, 147424}, {147427, 147436}, {147438, 147451}, {147453, 147454}, {147456, 147464}, {147467, 147492}, {147494, 147505}, {147508, 147512}, {147514, 147519}, {147521, 147523}, {147651, 147651}, {147724, 147737}, {147739, 147751}, {147753, 147757}, {147759, 147765}, {147767, 147785}, {147793, 147794}, {147805, 147806}, {147817, 147817}, {147825, 147826}, {147835, 147835}, {147837, 147837}, {147847, 147847}, {147907, 147907}, {148036, 148036}, {148162, 148162}, {148291, 148291}, {148419, 148419},
		{148547, 148547}, {148675, 148675}, {148803, 148803}},
	}
	raw := len(n.ranges)
	pkt, err := n.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if len(pkt) > DatagramSize {
		t.Errorf("packet size should be less than DatagramSize")
	}
	var r Nck
	err = r.Unmarshal(pkt)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.ranges) == 0 {
		t.Fatal("expected ranges to contain at least one range")
	}
	if len(r.ranges) == raw {
		t.Fatal("should truncate ranges")
	}
	if !reflect.DeepEqual(n, r) {
		t.Errorf("should be same")
	}
}

func TestNck(t *testing.T) {
	n := Nck{13751, 1804775552, "👿无心#23008", []Range{{227, 227}, {6390, 6396}, {6398, 6405}, {6425, 6430}, {6911, 6916}, {7299, 7299}, {13212, 13212}, {13282, 13284}, {13304, 13309}, {13976, 13976}, {14190, 14190}, {15102, 15102}, {15170, 15170}, {15931, 15931}, {16020, 16020}, {16647, 16647}, {16834, 16834}, {17452, 17452}, {17683, 18115}, {18117, 18379}, {18381, 18405}, {18407, 18853}, {18855, 18984}, {18986, 19007}, {19009, 19065}, {19067, 19096}, {19098, 19112}, {19273, 19273}, {20011, 20011}, {20537, 20537}, {21277, 21277}, {21331, 21343}, {21353, 21354}, {21382, 21382}, {21387, 21387}, {21404, 21404}, {21421, 21461}, {21526, 21526}, {21559, 21559}, {21576, 21576}, {21592, 21629}, {21671, 21682}, {21695, 21696}, {21704, 21721}, {21729, 21729}, {21742, 21756}, {21762, 21806}, {21853, 21853}, {21871, 21872}, {21879, 21895}, {21905, 21905}, {21922, 21922}, {21938, 21946}, {21948, 21991}, {22056, 22057}, {22090, 22090}, {22119, 22119}, {22123, 22163}, {22165, 22166}, {22231, 22232}, {22263, 22263}, {22265, 22265}, {22282, 22282}, {22298, 22433}, {22435, 22486}, {22488, 22489}, {22491, 22500}, {22502, 22519}, {22521, 22541}, {22543, 22655}, {22918, 22918}, {23240, 23240}, {23419, 23419}, {23683, 23683}, {23768, 23796}, {23798, 23828}, {23831, 23833}, {23835, 23836}, {23839, 23840}, {23847, 23848}, {23857, 23858}, {23861, 23861}, {23865, 23866}, {23875, 23876}, {23884, 23884}, {23891, 23892}, {23901, 23903}, {23908, 23909}, {23914, 23915}, {23918, 23919}, {23924, 23925}, {23928, 23930}, {23940, 23941}, {23944, 23944}, {24317, 24321}, {24330, 24330}, {25312, 25312}, {26406, 26427}, {26429, 26439}, {26459, 26484}, {26685, 26692}, {26776, 26782}, {28562, 28571}, {29063, 29071}, {29217, 29220}, {35380, 35380}, {38182, 38185}, {38261, 38289}, {38396, 38417}, {38419, 38452}, {38464, 38481}, {38621, 38621}, {38814, 38838}, {38840, 38852}, {42394, 42394}, {43388, 43388}, {44388, 44388}, {46192, 46192}, {46312, 46312}, {46537, 46537}, {46644, 46644}, {47515, 47515}, {47786, 47786}, {47812, 47812}, {48332, 48332}, {48376, 48376}, {48609, 48609}, {49080, 49080}, {49184, 49184}, {49227, 49253}, {49280, 49280}, {49594, 49594}, {49949, 49949}, {50030, 50030}, {50036, 50036}, {50147, 50147}, {50184, 50184}, {50604, 50604}, {51088, 51088}, {51216, 51242}, {51244, 51257}, {51277, 51312}, {51333, 51397}, {51408, 51412}, {51414, 51437}, {51457, 51487}, {51507, 51540}, {51560, 51585}, {51949, 51987}, {52022, 52146}, {52149, 52179}, {52182, 52206}, {52208, 52213}, {52218, 52223}, {52226, 52229}, {52234, 52239}, {52242, 52246}, {52612, 52612}, {52621, 52621}, {52862, 52862}, {52991, 53000}, {53209, 53209}, {53304, 53304}, {53535, 53535}, {53538, 53543}, {53560, 53560}, {53568, 53568}, {53624, 54636}, {54663, 54674}, {54756, 54780}, {55123, 55172}, {55192, 55193}, {55506, 57093}, {57095, 58693}, {58719, 58749}, {58802, 58802}, {58822, 68828}, {68830, 71064}, {71066, 72436}, {72438, 72459}, {72461, 75585}}}
	pkt, err := n.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if len(pkt) > DatagramSize {
		t.Errorf("packet size should be less than DatagramSize")
	}
}

func TestFileContentRangeMergeAndExclude(t *testing.T) {
	fc := fileContent{reading: new(RangeTracker), pending: new(RangeTracker)}
	fc.add([]Range{{50, 100}})
	if !fc.pending.isCompleted() {
		t.Errorf("pending should have been completed")
	}
	fc.add([]Range{{50, 100}})
	if len(fc.reading.ranges) != 1 {
		t.Errorf("ranges should have been excluded")
	}
	fc.add([]Range{{60, 80}})
	if len(fc.reading.ranges) != 1 {
		t.Errorf("ranges should have been excluded")
	}
	fc.add([]Range{{80, 120}})
	expected := []Range{{101, 120}}
	if !reflect.DeepEqual(expected, fc.pending.ranges) {
		t.Errorf("expected: %v, got: %v", expected, fc.pending.ranges)
	}
	fc.add([]Range{{80, 120}})
	if !reflect.DeepEqual(expected, fc.pending.ranges) {
		t.Errorf("expected: %v, got: %v", expected, fc.pending.ranges)
	}
	fc.add([]Range{{1, 120}})
	expected = []Range{{101, 120}, {1, 49}}
	if !reflect.DeepEqual(expected, fc.pending.ranges) {
		t.Errorf("expected: %v, got: %v", expected, fc.pending.ranges)
	}
	fc.swap()
	if !reflect.DeepEqual(expected, fc.reading.ranges) {
		t.Errorf("expected: %v, got: %v", expected, fc.reading.ranges)
	}
	fc.add([]Range{{1, 120}})
	if !reflect.DeepEqual(expected, fc.reading.ranges) {
		t.Errorf("expected: %v, got: %v", expected, fc.reading.ranges)
	}
	expected = []Range{{50, 100}}
	if !reflect.DeepEqual(expected, fc.pending.ranges) {
		t.Errorf("expected: %v, got: %v", expected, fc.pending.ranges)
	}
}

func TestRange(t *testing.T) {
	rt := RangeTracker{}
	rt.Track(Range{1, 20})
	if !rt.isCompleted() {
		t.Error("Expected RangeTracker to be completed")
	}
}

func TestRangeRemove(t *testing.T) {
	rt := RangeTracker{}
	rt.Track(Range{1, 20})
	rt.Track(Range{21, 21})
	// miss 22
	rt.Track(Range{23, 24})
	// miss 22, [25,49]
	rt.Track(Range{50, 50})
	expected := []Range{{22, 22}, {25, 49}}
	if !reflect.DeepEqual(rt.Get(), expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, rt.Get())
	}
	rt.Track(Range{20, 22})
	expected = []Range{{25, 49}}
	if !reflect.DeepEqual(rt.Get(), expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, rt.Get())
	}
	rt.Track(Range{25, 25})
	expected = []Range{{26, 49}}
	if !reflect.DeepEqual(rt.Get(), expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, rt.Get())
	}
	rt.Track(Range{45, 50})
	rt.Track(Range{20, 20})
	expected = []Range{{26, 44}}
	if !reflect.DeepEqual(rt.Get(), expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, rt.Get())
	}
	rt.Track(Range{25, 30})
	expected = []Range{{31, 44}}
	if !reflect.DeepEqual(rt.Get(), expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, rt.Get())
	}
	// miss [31,35],[40,44]
	rt.Track(Range{36, 39})
	expected = []Range{{31, 35}, {40, 44}}
	if !reflect.DeepEqual(rt.Get(), expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, rt.Get())
	}
	rt.Track(Range{38, 38})
	rt.Track(Range{31, 35})
	rt.Track(Range{40, 44})
	if !rt.isCompleted() {
		t.Error("Expected RangeTracker to be completed")
	}
}

func TestRangeAdd(t *testing.T) {
	rt := RangeTracker{}
	rt.Track(Range{1, 20})
	rt.Track(Range{1, 20})
	rt.Track(Range{30, 40})
	rt.Track(Range{40, 50})
	rt.Track(Range{40, 45})
	expected := []Range{{21, 29}}
	if !reflect.DeepEqual(rt.Get(), expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, rt.Get())
	}
	rt.Track(rt.ranges[0])
	if !rt.isCompleted() {
		t.Error("Expected RangeTracker to be completed")
	}
}

func TestFullRange(t *testing.T) {
	rt := RangeTracker{}
	rt.Track(FullRange)
	if !rt.isCompleted() {
		t.Error("Expected RangeTracker to be completed")
	}
	if rt.nextBlock() != math.MaxUint32 {
		t.Error("Expected RangeTracker to have MaxUint32 as next block")
	}
}

func TestRangeContains(t *testing.T) {
	rt := RangeTracker{}
	rt.Track(Range{1, 20})
	if !rt.Contains(Range{1, 20}) || !rt.Contains(Range{1, 1}) || !rt.Contains(Range{20, 20}) {
		t.Error("Expected RangeTracker to contain Range")
	}
	if rt.Contains(MonoRange(21)) {
		t.Error("Expected RangeTracker to not contain MonoRange(21)")
	}
	if rt.Contains(Range{1, 21}) {
		t.Error("Expected RangeTracker to not contain Range{1,21}")
	}
	rt.Track(Range{23, 23})
	if rt.Contains(MonoRange(22)) {
		t.Error("Expected RangeTracker to not contain MonoRange(22)")
	}
	rt.Track(MonoRange(21))
	if !rt.Contains(MonoRange(21)) {
		t.Error("Expected RangeTracker to contain MonoRange(21)")
	}
	if rt.Contains(MonoRange(22)) {
		t.Error("Expected RangeTracker to not contain MonoRange(22)")
	}
	if rt.Contains(Range{21, 23}) {
		t.Error("Expected RangeTracker to not contain Range{21,23}")
	}
}

func TestRangeSelect(t *testing.T) {
	tracker := new(RangeTracker)
	tracker.Track(Range{1, 1})
	tracker.Track(Range{3, 3})
	tracker.Track(Range{10, 20})
	// missing [2,2],[4,9]
	r := tracker.Select(Range{2, 4})
	expected := []Range{{2, 2}, {4, 4}}
	if !reflect.DeepEqual(r, expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, r)
	}
	r = tracker.Select(Range{6, 6})
	expected = []Range{{6, 6}}
	if !reflect.DeepEqual(r, expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, r)
	}
	r = tracker.Select(Range{8, 10})
	expected = []Range{{8, 9}}
	if !reflect.DeepEqual(r, expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, r)
	}
}

func TestMonoRange(t *testing.T) {
	rt := RangeTracker{}
	rt.Track(MonoRange(4))
	rt.Track(MonoRange(2))
	expected := []Range{MonoRange(1), MonoRange(3)}
	if !reflect.DeepEqual(rt.Get(), expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, rt.Get())
	}
	rt.Track(MonoRange(6))
	expected = []Range{MonoRange(1), MonoRange(3), MonoRange(5)}
	if !reflect.DeepEqual(rt.Get(), expected) {
		t.Errorf("Expected RangeTracker %v, got %v", expected, rt.Get())
	}
	rt.Track(MonoRange(1))
	rt.Track(MonoRange(3))
	rt.Track(MonoRange(5))
	if !rt.isCompleted() {
		t.Error("Expected RangeTracker to be completed")
	}
	rt.Track(Range{1, 6})
	if !rt.isCompleted() {
		t.Error("Expected RangeTracker to be completed")
	}
}

func TestNckMarshalAndUnmarshal(t *testing.T) {
	ranges := []Range{{1, 1}, {3, 5}, {8, 8}}
	nck := Nck{FileId: 1, ranges: ranges}
	pkt, err := nck.Marshal()
	if err != nil {
		t.Error(err)
	}
	var ret Nck
	err = ret.Unmarshal(pkt)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(nck, ret) {
		t.Errorf("Expected %v, got %v", nck, ret)
	}
}

func TestCtrlReq(t *testing.T) {
	req := CtrlReq{OpSyncName, 1, "old", "new"}
	pkt, err := req.Marshal()
	if err != nil {
		t.Error(err)
	}
	var r CtrlReq
	err = r.Unmarshal(pkt)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(req, r) {
		t.Errorf("Expected %v, got %v", req, r)
	}
}

func TestSignedMessage(t *testing.T) {
	msg := SignedMessage{SignReq: SignReq{1, SignBody{Sign: "default", UUID: "test"}}, Payload: []byte("hello")}
	pkt, err := msg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var m SignedMessage
	err = m.Unmarshal(pkt)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(msg, m) {
		t.Errorf("expected %v; actual %v", msg, m)
	}
}
