/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @record
 */
export function ClickPosition() { }
if (false) {
    /** @type {?} */
    ClickPosition.prototype.x;
    /** @type {?} */
    ClickPosition.prototype.y;
}
export class ModalUtil {
    /**
     * @param {?} document
     */
    constructor(document) {
        this.document = document;
        this.lastPosition = null;
        this.listenDocumentClick();
    }
    /**
     * @return {?}
     */
    getLastClickPosition() {
        return this.lastPosition;
    }
    /**
     * @return {?}
     */
    listenDocumentClick() {
        this.document.addEventListener('click', (/**
         * @param {?} event
         * @return {?}
         */
        (event) => {
            this.lastPosition = { x: event.clientX, y: event.clientY };
        }));
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ModalUtil.prototype.lastPosition;
    /**
     * @type {?}
     * @private
     */
    ModalUtil.prototype.document;
}
export default new ModalUtil(document);
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibW9kYWwtdXRpbC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJtb2RhbC9tb2RhbC11dGlsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7QUFBQSxtQ0FHQzs7O0lBRkMsMEJBQVU7O0lBQ1YsMEJBQVU7O0FBR1osTUFBTSxPQUFPLFNBQVM7Ozs7SUFHcEIsWUFBb0IsUUFBa0I7UUFBbEIsYUFBUSxHQUFSLFFBQVEsQ0FBVTtRQUY5QixpQkFBWSxHQUF5QixJQUFJLENBQUM7UUFHaEQsSUFBSSxDQUFDLG1CQUFtQixFQUFFLENBQUM7SUFDN0IsQ0FBQzs7OztJQUVELG9CQUFvQjtRQUNsQixPQUFPLElBQUksQ0FBQyxZQUFZLENBQUM7SUFDM0IsQ0FBQzs7OztJQUVELG1CQUFtQjtRQUNqQixJQUFJLENBQUMsUUFBUSxDQUFDLGdCQUFnQixDQUFDLE9BQU87Ozs7UUFBRSxDQUFDLEtBQWlCLEVBQUUsRUFBRTtZQUM1RCxJQUFJLENBQUMsWUFBWSxHQUFHLEVBQUUsQ0FBQyxFQUFFLEtBQUssQ0FBQyxPQUFPLEVBQUUsQ0FBQyxFQUFFLEtBQUssQ0FBQyxPQUFPLEVBQUUsQ0FBQztRQUM3RCxDQUFDLEVBQUMsQ0FBQztJQUNMLENBQUM7Q0FDRjs7Ozs7O0lBZkMsaUNBQWtEOzs7OztJQUV0Qyw2QkFBMEI7O0FBZXhDLGVBQWUsSUFBSSxTQUFTLENBQUMsUUFBUSxDQUFDLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJleHBvcnQgaW50ZXJmYWNlIENsaWNrUG9zaXRpb24ge1xuICB4OiBudW1iZXI7XG4gIHk6IG51bWJlcjtcbn1cblxuZXhwb3J0IGNsYXNzIE1vZGFsVXRpbCB7XG4gIHByaXZhdGUgbGFzdFBvc2l0aW9uOiBDbGlja1Bvc2l0aW9uIHwgbnVsbCA9IG51bGw7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBkb2N1bWVudDogRG9jdW1lbnQpIHtcbiAgICB0aGlzLmxpc3RlbkRvY3VtZW50Q2xpY2soKTtcbiAgfVxuXG4gIGdldExhc3RDbGlja1Bvc2l0aW9uKCk6IENsaWNrUG9zaXRpb24gfCBudWxsIHtcbiAgICByZXR1cm4gdGhpcy5sYXN0UG9zaXRpb247XG4gIH1cblxuICBsaXN0ZW5Eb2N1bWVudENsaWNrKCk6IHZvaWQge1xuICAgIHRoaXMuZG9jdW1lbnQuYWRkRXZlbnRMaXN0ZW5lcignY2xpY2snLCAoZXZlbnQ6IE1vdXNlRXZlbnQpID0+IHtcbiAgICAgIHRoaXMubGFzdFBvc2l0aW9uID0geyB4OiBldmVudC5jbGllbnRYLCB5OiBldmVudC5jbGllbnRZIH07XG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGRlZmF1bHQgbmV3IE1vZGFsVXRpbChkb2N1bWVudCk7XG4iXX0=