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
var ModalUtil = /** @class */ (function () {
    function ModalUtil(document) {
        this.document = document;
        this.lastPosition = null;
        this.listenDocumentClick();
    }
    /**
     * @return {?}
     */
    ModalUtil.prototype.getLastClickPosition = /**
     * @return {?}
     */
    function () {
        return this.lastPosition;
    };
    /**
     * @return {?}
     */
    ModalUtil.prototype.listenDocumentClick = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.document.addEventListener('click', (/**
         * @param {?} event
         * @return {?}
         */
        function (event) {
            _this.lastPosition = { x: event.clientX, y: event.clientY };
        }));
    };
    return ModalUtil;
}());
export { ModalUtil };
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibW9kYWwtdXRpbC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJtb2RhbC9tb2RhbC11dGlsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7QUFBQSxtQ0FHQzs7O0lBRkMsMEJBQVU7O0lBQ1YsMEJBQVU7O0FBR1o7SUFHRSxtQkFBb0IsUUFBa0I7UUFBbEIsYUFBUSxHQUFSLFFBQVEsQ0FBVTtRQUY5QixpQkFBWSxHQUF5QixJQUFJLENBQUM7UUFHaEQsSUFBSSxDQUFDLG1CQUFtQixFQUFFLENBQUM7SUFDN0IsQ0FBQzs7OztJQUVELHdDQUFvQjs7O0lBQXBCO1FBQ0UsT0FBTyxJQUFJLENBQUMsWUFBWSxDQUFDO0lBQzNCLENBQUM7Ozs7SUFFRCx1Q0FBbUI7OztJQUFuQjtRQUFBLGlCQUlDO1FBSEMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxnQkFBZ0IsQ0FBQyxPQUFPOzs7O1FBQUUsVUFBQyxLQUFpQjtZQUN4RCxLQUFJLENBQUMsWUFBWSxHQUFHLEVBQUUsQ0FBQyxFQUFFLEtBQUssQ0FBQyxPQUFPLEVBQUUsQ0FBQyxFQUFFLEtBQUssQ0FBQyxPQUFPLEVBQUUsQ0FBQztRQUM3RCxDQUFDLEVBQUMsQ0FBQztJQUNMLENBQUM7SUFDSCxnQkFBQztBQUFELENBQUMsQUFoQkQsSUFnQkM7Ozs7Ozs7SUFmQyxpQ0FBa0Q7Ozs7O0lBRXRDLDZCQUEwQjs7QUFleEMsZUFBZSxJQUFJLFNBQVMsQ0FBQyxRQUFRLENBQUMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbImV4cG9ydCBpbnRlcmZhY2UgQ2xpY2tQb3NpdGlvbiB7XG4gIHg6IG51bWJlcjtcbiAgeTogbnVtYmVyO1xufVxuXG5leHBvcnQgY2xhc3MgTW9kYWxVdGlsIHtcbiAgcHJpdmF0ZSBsYXN0UG9zaXRpb246IENsaWNrUG9zaXRpb24gfCBudWxsID0gbnVsbDtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGRvY3VtZW50OiBEb2N1bWVudCkge1xuICAgIHRoaXMubGlzdGVuRG9jdW1lbnRDbGljaygpO1xuICB9XG5cbiAgZ2V0TGFzdENsaWNrUG9zaXRpb24oKTogQ2xpY2tQb3NpdGlvbiB8IG51bGwge1xuICAgIHJldHVybiB0aGlzLmxhc3RQb3NpdGlvbjtcbiAgfVxuXG4gIGxpc3RlbkRvY3VtZW50Q2xpY2soKTogdm9pZCB7XG4gICAgdGhpcy5kb2N1bWVudC5hZGRFdmVudExpc3RlbmVyKCdjbGljaycsIChldmVudDogTW91c2VFdmVudCkgPT4ge1xuICAgICAgdGhpcy5sYXN0UG9zaXRpb24gPSB7IHg6IGV2ZW50LmNsaWVudFgsIHk6IGV2ZW50LmNsaWVudFkgfTtcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgZGVmYXVsdCBuZXcgTW9kYWxVdGlsKGRvY3VtZW50KTtcbiJdfQ==