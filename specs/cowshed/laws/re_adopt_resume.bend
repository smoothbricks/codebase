# Re-adoption's resume decisions (specs/cowshed/02_workspaces.md "Re-adoption: space reclamation" step 3 and
# "Missing main image"): which build a missing main takes, which build publication a recorded re-adoption runs,
# and how an empty build or a build replacement converges from what a crash left at its id. Each decision reads
# only facts production already reads (whether an image exists, the record at an id compared with the
# re-adoption's registration, the count of `Linked main` records, the claim the caller holds) and answers one
# step or one refusal. The pure core is cowshed-core src/storage/recovery/resume.rs; the I/O around it runs
# exactly the step it answers.
#
# Check: bend2 specs/cowshed/laws/re_adopt_resume.bend --verdict
# Bound to the code by cowshed-core src/storage/recovery/resume/laws.rs, which runs the emitted model
# (resume/laws/model.rs) and the code on every input of the finite domain.
import Base

# The record found at a build volume id, against the re-adoption's own registration of that id.
type Rec is Data:
  RNone{}
  RRegistered{}
  RPublished{}
  RForeign{}

# --- an empty build published under the recorded id ------------------------------------------------------

type EmptyStep is Data:
  ECreate{}
  EMount{}
  ENoRecord{}
  EForeign{}
  ENoImage{}

# `image`: an image exists at the id. Nothing is published over an image this re-adoption did not register,
# and no record is written that names a missing image (re_adopt.rs `publish_empty_build`).
def empty_step(image: Bool, rec: Rec) -> EmptyStep:
  match image rec:
    case False{} RNone{}:
      ECreate{}
    case False{} RRegistered{}:
      ECreate{}
    case False{} RPublished{}:
      ENoImage{}
    case False{} RForeign{}:
      ENoImage{}
    case True{} RNone{}:
      ENoRecord{}
    case True{} RRegistered{}:
      EMount{}
    case True{} RPublished{}:
      EMount{}
    case True{} RForeign{}:
      EForeign{}

# --- a verified build replacement published under the recorded id ----------------------------------------

type RepStep is Data:
  PRename{}
  PAlready{}
  POccupied{}
  PForeign{}
  PNoRecord{}
  PNothing{}

# `staged`: the staged replacement image exists; `placed`: an image exists at the build image path
# (re_adopt.rs `publish_build_replacement`).
def rep_step(staged: Bool, placed: Bool, rec: Rec) -> RepStep:
  match staged placed:
    case True{} True{}:
      POccupied{}
    case True{} False{}:
      PRename{}
    case False{} False{}:
      PNothing{}
    case False{} True{}:
      match rec:
        case RNone{}:
          PNoRecord{}
        case RRegistered{}:
          PAlready{}
        case RPublished{}:
          PAlready{}
        case RForeign{}:
          PForeign{}

# --- which build publication a recorded re-adoption runs --------------------------------------------------

# A missing main's build, as its record chose it.
type MissingBuild is Data:
  MNoBuild{}
  MEmpty{}
  MKept{}

type Main is Data:
  Missing{b: MissingBuild}
  Retained{}

# The claim the caller holds on a source build's hold.
type Claim is Data:
  CNone{}
  CMatch{}
  COther{}

type Publication is Data:
  BNothing{}
  BEmptyUnclaimed{}
  BKeptUnclaimed{}
  BReplacement{}
  BLostEmptyClaimed{}
  BInvalid{}

# `build`: the record names a source build; `retained`: its retained image exists; `published`: an earlier
# attempt published its verified replacement (re_adopt.rs `replacement_published`). Once published, the
# replacement never reads its retained image again, so losing that image keeps it; lost before publication,
# the build is published empty under the claim the caller holds (re_adopt.rs `publish_re_adopt_build`).
def publication(main: Main, build: Bool, claim: Claim, retained: Bool, published: Bool) -> Publication:
  match main build claim:
    case Missing{MNoBuild{}} False{} CNone{}:
      BNothing{}
    case Missing{MEmpty{}} False{} CNone{}:
      BEmptyUnclaimed{}
    case Missing{MKept{}} False{} CNone{}:
      BKeptUnclaimed{}
    case Retained{} False{} CNone{}:
      BNothing{}
    case Retained{} True{} CMatch{}:
      match retained published:
        case False{} False{}:
          BLostEmptyClaimed{}
        case False{} True{}:
          BReplacement{}
        case True{} p:
          BReplacement{}
    case m b c:
      BInvalid{}

# --- a missing main's build ---------------------------------------------------------------------------------

# How many build records name `Linked main`.
type Linked is Data:
  LNone{}
  LOne{}
  LMany{}

type MissingChoice is Data:
  CNoBuild{}
  CEmpty{}
  CKept{}
  CConflict{}
  CIntegrity{}

# `recorded`: the completed re-adoption names a build volume; `occupied`: an image or a record is already at
# that id (project_re_adopt.rs `record_missing_main`). A surviving `Linked main` volume is kept whatever its
# id; only when none survives is the recorded one published empty, and never over a volume.
def missing_choice(linked: Linked, recorded: Bool, occupied: Bool) -> MissingChoice:
  match linked:
    case LOne{}:
      CKept{}
    case LMany{}:
      CIntegrity{}
    case LNone{}:
      match recorded occupied:
        case False{} o:
          CNoBuild{}
        case True{} False{}:
          CEmpty{}
        case True{} True{}:
          CConflict{}

# --- the build copy before publication ---------------------------------------------------------------------

type Copy is Data:
  KNothing{}
  KCopy{}
  KDiscard{}

# A recorded build whose retained image is gone is lost: its staged copy can no longer be verified and is
# discarded (re_adopt.rs `copy_re_adopt_build`).
def copy_step(build: Bool, retained: Bool) -> Copy:
  match build retained:
    case False{} r:
      KNothing{}
    case True{} True{}:
      KCopy{}
    case True{} False{}:
      KDiscard{}

# --- predicates the laws read --------------------------------------------------------------------------------

def empty_writes(s: EmptyStep) -> Bool:
  match s:
    case ECreate{}:
      True{}
    case EMount{}:
      True{}
    case ENoRecord{}:
      False{}
    case EForeign{}:
      False{}
    case ENoImage{}:
      False{}

def rep_writes(s: RepStep) -> Bool:
  match s:
    case PRename{}:
      True{}
    case PAlready{}:
      True{}
    case POccupied{}:
      False{}
    case PForeign{}:
      False{}
    case PNoRecord{}:
      False{}
    case PNothing{}:
      False{}

def is_foreign_or_none(r: Rec) -> Bool:
  match r:
    case RNone{}:
      True{}
    case RRegistered{}:
      False{}
    case RPublished{}:
      False{}
    case RForeign{}:
      True{}

def is_ours(r: Rec) -> Bool:
  match r:
    case RNone{}:
      False{}
    case RRegistered{}:
      True{}
    case RPublished{}:
      True{}
    case RForeign{}:
      False{}

# Only a missing record or this re-adoption's own registration may precede the image a create mints.
def creatable(r: Rec) -> Bool:
  match r:
    case RNone{}:
      True{}
    case RRegistered{}:
      True{}
    case RPublished{}:
      False{}
    case RForeign{}:
      False{}

def is_match(c: Claim) -> Bool:
  match c:
    case CNone{}:
      False{}
    case CMatch{}:
      True{}
    case COther{}:
      False{}
def is_empty_choice(c: MissingChoice) -> Bool:
  match c:
    case CNoBuild{}:
      False{}
    case CEmpty{}:
      True{}
    case CKept{}:
      False{}
    case CConflict{}:
      False{}
    case CIntegrity{}:
      False{}
def is_invalid(p: Publication) -> Bool:
  match p:
    case BInvalid{}:
      True{}
    case BNothing{}:
      False{}
    case BEmptyUnclaimed{}:
      False{}
    case BKeptUnclaimed{}:
      False{}
    case BReplacement{}:
      False{}
    case BLostEmptyClaimed{}:
      False{}

# --- laws ---------------------------------------------------------------------------------------------------

# Nothing is published over an image another owner registered, or that no record accounts for.
law empty_never_over_a_foreign_image:
  for +rec: Rec
  {Bool.and(empty_writes(empty_step(True{}, rec)), is_foreign_or_none(rec)) == False{} : Bool}

def empty_never_over_a_foreign_image(rec):
  match rec:
    case RNone{}:
      {==}
    case RRegistered{}:
      {==}
    case RPublished{}:
      {==}
    case RForeign{}:
      {==}

law replacement_never_over_a_foreign_image:
  for +staged: Bool
  for +rec: Rec
  {Bool.and(rep_writes(rep_step(staged, True{}, rec)), is_foreign_or_none(rec)) == False{} : Bool}

def replacement_never_over_a_foreign_image(staged, rec):
  match staged rec:
    case True{} r:
      {==}
    case False{} RNone{}:
      {==}
    case False{} RRegistered{}:
      {==}
    case False{} RPublished{}:
      {==}
    case False{} RForeign{}:
      {==}

# No record is published that names a missing image: with no image at the id, only a missing record or the
# re-adoption's own registration is created over, and a replacement neither staged nor placed writes nothing.
law empty_no_record_names_nothing:
  for +rec: Rec
  {empty_writes(empty_step(False{}, rec)) == creatable(rec) : Bool}

def empty_no_record_names_nothing(rec):
  match rec:
    case RNone{}:
      {==}
    case RRegistered{}:
      {==}
    case RPublished{}:
      {==}
    case RForeign{}:
      {==}

law replacement_no_record_names_nothing:
  for +rec: Rec
  {rep_writes(rep_step(False{}, False{}, rec)) == False{} : Bool}

def replacement_no_record_names_nothing(rec):
  {==}

# A replay converges: an image whose record is this re-adoption's own, registered or published, is taken as
# it is, by the empty build and by the replacement alike.
law empty_replay_converges:
  for +rec: Rec
  {empty_writes(empty_step(True{}, rec)) == is_ours(rec) : Bool}

def empty_replay_converges(rec):
  match rec:
    case RNone{}:
      {==}
    case RRegistered{}:
      {==}
    case RPublished{}:
      {==}
    case RForeign{}:
      {==}

law replacement_replay_converges:
  for +rec: Rec
  {rep_writes(rep_step(False{}, True{}, rec)) == is_ours(rec) : Bool}

def replacement_replay_converges(rec):
  match rec:
    case RNone{}:
      {==}
    case RRegistered{}:
      {==}
    case RPublished{}:
      {==}
    case RForeign{}:
      {==}
# A published replacement is kept whatever became of its retained image; only an unpublished build whose
# retained image is gone is published empty.
law published_replacement_is_kept:
  for +retained: Bool
  {publication(Retained{}, True{}, CMatch{}, retained, True{}) == BReplacement{} : Publication}

def published_replacement_is_kept(retained):
  match retained:
    case True{}:
      {==}
    case False{}:
      {==}

# A retained main's build is published only under the claim that names it; a missing main holds none.
law a_retained_build_needs_its_claim:
  for +claim: Claim
  for +retained: Bool
  for +published: Bool
  {is_invalid(publication(Retained{}, True{}, claim, retained, published)) == Bool.not(is_match(claim)) : Bool}

def a_retained_build_needs_its_claim(claim, retained, published):
  match claim retained published:
    case CNone{} r p:
      {==}
    case COther{} r p:
      {==}
    case CMatch{} True{} p:
      {==}
    case CMatch{} False{} True{}:
      {==}
    case CMatch{} False{} False{}:
      {==}
law a_missing_main_holds_no_claim:
  for +b: MissingBuild
  for +retained: Bool
  for +published: Bool
  {is_invalid(publication(Missing{b}, False{}, CMatch{}, retained, published)) == True{} : Bool}

def a_missing_main_holds_no_claim(b, retained, published):
  match b:
    case MNoBuild{}:
      {==}
    case MEmpty{}:
      {==}
    case MKept{}:
      {==}

# A missing main never publishes over a build volume still at the recorded id.
law a_missing_main_never_publishes_over_a_volume:
  for +occupied: Bool
  {Bool.and(occupied, is_empty_choice(missing_choice(LNone{}, True{}, occupied))) == False{} : Bool}

def a_missing_main_never_publishes_over_a_volume(occupied):
  match occupied:
    case True{}:
      {==}
    case False{}:
      {==}
