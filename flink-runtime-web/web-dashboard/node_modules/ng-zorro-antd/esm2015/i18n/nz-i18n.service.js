/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Inject, Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import zh_CN from './languages/zh_CN';
import { NZ_DATE_LOCALE, NZ_I18N } from './nz-i18n.token';
import * as i0 from "@angular/core";
import * as i1 from "./public-api";
export class NzI18nService {
    /**
     * @param {?} locale
     * @param {?} dateLocale
     */
    constructor(locale, dateLocale) {
        this._change = new BehaviorSubject(this._locale);
        this.setLocale(locale || zh_CN);
        this.setDateLocale(dateLocale || null);
    }
    /**
     * @return {?}
     */
    get localeChange() {
        return this._change.asObservable();
    }
    // [NOTE] Performance issue: this method may called by every change detections
    // TODO: cache more deeply paths for performance
    /* tslint:disable-next-line:no-any */
    /**
     * @param {?} path
     * @param {?=} data
     * @return {?}
     */
    translate(path, data) {
        // this._logger.debug(`[NzI18nService] Translating(${this._locale.locale}): ${path}`);
        /** @type {?} */
        let content = (/** @type {?} */ (this._getObjectPath(this._locale, path)));
        if (typeof content === 'string') {
            if (data) {
                Object.keys(data).forEach((/**
                 * @param {?} key
                 * @return {?}
                 */
                key => (content = content.replace(new RegExp(`%${key}%`, 'g'), data[key]))));
            }
            return content;
        }
        return path;
    }
    /**
     * Set/Change current locale globally throughout the WHOLE application
     * [NOTE] If called at runtime, rendered interface may not change along with the locale change (because this do not trigger another render schedule)
     * @param {?} locale The translating letters
     * @return {?}
     */
    setLocale(locale) {
        if (this._locale && this._locale.locale === locale.locale) {
            return;
        }
        this._locale = locale;
        this._change.next(locale);
    }
    /**
     * @return {?}
     */
    getLocale() {
        return this._locale;
    }
    /**
     * @return {?}
     */
    getLocaleId() {
        return this._locale ? this._locale.locale : '';
    }
    /**
     * @param {?} dateLocale
     * @return {?}
     */
    setDateLocale(dateLocale) {
        this.dateLocale = dateLocale;
    }
    /**
     * @return {?}
     */
    getDateLocale() {
        return this.dateLocale;
    }
    /**
     * Get locale data
     * @param {?=} path dot paths for finding exist value from locale data, eg. "a.b.c"
     * @param {?=} defaultValue default value if the result is not "truthy"
     * @return {?}
     */
    // tslint:disable-next-line:no-any
    getLocaleData(path, defaultValue) {
        /** @type {?} */
        const result = path ? this._getObjectPath(this._locale, path) : this._locale;
        return result || defaultValue;
    }
    // tslint:disable-next-line:no-any
    /**
     * @private
     * @param {?} obj
     * @param {?} path
     * @return {?}
     */
    _getObjectPath(obj, path) {
        /** @type {?} */
        let res = obj;
        /** @type {?} */
        const paths = path.split('.');
        /** @type {?} */
        const depth = paths.length;
        /** @type {?} */
        let index = 0;
        while (res && index < depth) {
            res = res[paths[index++]];
        }
        return index === depth ? res : null;
    }
}
NzI18nService.decorators = [
    { type: Injectable, args: [{
                providedIn: 'root'
            },] }
];
/** @nocollapse */
NzI18nService.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [NZ_I18N,] }] },
    { type: undefined, decorators: [{ type: Inject, args: [NZ_DATE_LOCALE,] }] }
];
/** @nocollapse */ NzI18nService.ngInjectableDef = i0.defineInjectable({ factory: function NzI18nService_Factory() { return new i1.NzI18nService(i0.inject(i1.NZ_I18N), i0.inject(i1.NZ_DATE_LOCALE)); }, token: i1.NzI18nService, providedIn: "root" });
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzI18nService.prototype._locale;
    /**
     * @type {?}
     * @private
     */
    NzI18nService.prototype._change;
    /**
     * @type {?}
     * @private
     */
    NzI18nService.prototype.dateLocale;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotaTE4bi5zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImkxOG4vbnotaTE4bi5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsTUFBTSxFQUFFLFVBQVUsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUNuRCxPQUFPLEVBQUUsZUFBZSxFQUFjLE1BQU0sTUFBTSxDQUFDO0FBR25ELE9BQU8sS0FBSyxNQUFNLG1CQUFtQixDQUFDO0FBRXRDLE9BQU8sRUFBRSxjQUFjLEVBQUUsT0FBTyxFQUFFLE1BQU0saUJBQWlCLENBQUM7OztBQUsxRCxNQUFNLE9BQU8sYUFBYTs7Ozs7SUFTeEIsWUFBNkIsTUFBdUIsRUFBMEIsVUFBc0I7UUFQNUYsWUFBTyxHQUFHLElBQUksZUFBZSxDQUFrQixJQUFJLENBQUMsT0FBTyxDQUFDLENBQUM7UUFRbkUsSUFBSSxDQUFDLFNBQVMsQ0FBQyxNQUFNLElBQUksS0FBSyxDQUFDLENBQUM7UUFDaEMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxVQUFVLElBQUksSUFBSSxDQUFDLENBQUM7SUFDekMsQ0FBQzs7OztJQVBELElBQUksWUFBWTtRQUNkLE9BQU8sSUFBSSxDQUFDLE9BQU8sQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUNyQyxDQUFDOzs7Ozs7Ozs7SUFVRCxTQUFTLENBQUMsSUFBWSxFQUFFLElBQVU7OztZQUU1QixPQUFPLEdBQUcsbUJBQUEsSUFBSSxDQUFDLGNBQWMsQ0FBQyxJQUFJLENBQUMsT0FBTyxFQUFFLElBQUksQ0FBQyxFQUFVO1FBQy9ELElBQUksT0FBTyxPQUFPLEtBQUssUUFBUSxFQUFFO1lBQy9CLElBQUksSUFBSSxFQUFFO2dCQUNSLE1BQU0sQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsT0FBTzs7OztnQkFBQyxHQUFHLENBQUMsRUFBRSxDQUFDLENBQUMsT0FBTyxHQUFHLE9BQU8sQ0FBQyxPQUFPLENBQUMsSUFBSSxNQUFNLENBQUMsSUFBSSxHQUFHLEdBQUcsRUFBRSxHQUFHLENBQUMsRUFBRSxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxFQUFDLENBQUM7YUFDdkc7WUFDRCxPQUFPLE9BQU8sQ0FBQztTQUNoQjtRQUNELE9BQU8sSUFBSSxDQUFDO0lBQ2QsQ0FBQzs7Ozs7OztJQU9ELFNBQVMsQ0FBQyxNQUF1QjtRQUMvQixJQUFJLElBQUksQ0FBQyxPQUFPLElBQUksSUFBSSxDQUFDLE9BQU8sQ0FBQyxNQUFNLEtBQUssTUFBTSxDQUFDLE1BQU0sRUFBRTtZQUN6RCxPQUFPO1NBQ1I7UUFDRCxJQUFJLENBQUMsT0FBTyxHQUFHLE1BQU0sQ0FBQztRQUN0QixJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsQ0FBQztJQUM1QixDQUFDOzs7O0lBRUQsU0FBUztRQUNQLE9BQU8sSUFBSSxDQUFDLE9BQU8sQ0FBQztJQUN0QixDQUFDOzs7O0lBRUQsV0FBVztRQUNULE9BQU8sSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztJQUNqRCxDQUFDOzs7OztJQUVELGFBQWEsQ0FBQyxVQUFzQjtRQUNsQyxJQUFJLENBQUMsVUFBVSxHQUFHLFVBQVUsQ0FBQztJQUMvQixDQUFDOzs7O0lBRUQsYUFBYTtRQUNYLE9BQU8sSUFBSSxDQUFDLFVBQVUsQ0FBQztJQUN6QixDQUFDOzs7Ozs7OztJQVFELGFBQWEsQ0FBQyxJQUFhLEVBQUUsWUFBa0I7O2NBQ3ZDLE1BQU0sR0FBRyxJQUFJLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxjQUFjLENBQUMsSUFBSSxDQUFDLE9BQU8sRUFBRSxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLE9BQU87UUFDNUUsT0FBTyxNQUFNLElBQUksWUFBWSxDQUFDO0lBQ2hDLENBQUM7Ozs7Ozs7O0lBR08sY0FBYyxDQUFDLEdBQW9CLEVBQUUsSUFBWTs7WUFDbkQsR0FBRyxHQUFHLEdBQUc7O2NBQ1AsS0FBSyxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDOztjQUN2QixLQUFLLEdBQUcsS0FBSyxDQUFDLE1BQU07O1lBQ3RCLEtBQUssR0FBRyxDQUFDO1FBQ2IsT0FBTyxHQUFHLElBQUksS0FBSyxHQUFHLEtBQUssRUFBRTtZQUMzQixHQUFHLEdBQUcsR0FBRyxDQUFDLEtBQUssQ0FBQyxLQUFLLEVBQUUsQ0FBQyxDQUFDLENBQUM7U0FDM0I7UUFDRCxPQUFPLEtBQUssS0FBSyxLQUFLLENBQUMsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO0lBQ3RDLENBQUM7OztZQWxGRixVQUFVLFNBQUM7Z0JBQ1YsVUFBVSxFQUFFLE1BQU07YUFDbkI7Ozs7NENBVWMsTUFBTSxTQUFDLE9BQU87NENBQTRCLE1BQU0sU0FBQyxjQUFjOzs7Ozs7OztJQVI1RSxnQ0FBaUM7Ozs7O0lBQ2pDLGdDQUFxRTs7Ozs7SUFDckUsbUNBQStCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgSW5qZWN0LCBJbmplY3RhYmxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBCZWhhdmlvclN1YmplY3QsIE9ic2VydmFibGUgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IEluZGV4YWJsZU9iamVjdCB9IGZyb20gJy4uL2NvcmUvdHlwZXMvaW5kZXhhYmxlJztcblxuaW1wb3J0IHpoX0NOIGZyb20gJy4vbGFuZ3VhZ2VzL3poX0NOJztcbmltcG9ydCB7IERhdGVMb2NhbGUsIE56STE4bkludGVyZmFjZSB9IGZyb20gJy4vbnotaTE4bi5pbnRlcmZhY2UnO1xuaW1wb3J0IHsgTlpfREFURV9MT0NBTEUsIE5aX0kxOE4gfSBmcm9tICcuL256LWkxOG4udG9rZW4nO1xuXG5ASW5qZWN0YWJsZSh7XG4gIHByb3ZpZGVkSW46ICdyb290J1xufSlcbmV4cG9ydCBjbGFzcyBOekkxOG5TZXJ2aWNlIHtcbiAgcHJpdmF0ZSBfbG9jYWxlOiBOekkxOG5JbnRlcmZhY2U7XG4gIHByaXZhdGUgX2NoYW5nZSA9IG5ldyBCZWhhdmlvclN1YmplY3Q8TnpJMThuSW50ZXJmYWNlPih0aGlzLl9sb2NhbGUpO1xuICBwcml2YXRlIGRhdGVMb2NhbGU6IERhdGVMb2NhbGU7XG5cbiAgZ2V0IGxvY2FsZUNoYW5nZSgpOiBPYnNlcnZhYmxlPE56STE4bkludGVyZmFjZT4ge1xuICAgIHJldHVybiB0aGlzLl9jaGFuZ2UuYXNPYnNlcnZhYmxlKCk7XG4gIH1cblxuICBjb25zdHJ1Y3RvcihASW5qZWN0KE5aX0kxOE4pIGxvY2FsZTogTnpJMThuSW50ZXJmYWNlLCBASW5qZWN0KE5aX0RBVEVfTE9DQUxFKSBkYXRlTG9jYWxlOiBEYXRlTG9jYWxlKSB7XG4gICAgdGhpcy5zZXRMb2NhbGUobG9jYWxlIHx8IHpoX0NOKTtcbiAgICB0aGlzLnNldERhdGVMb2NhbGUoZGF0ZUxvY2FsZSB8fCBudWxsKTtcbiAgfVxuXG4gIC8vIFtOT1RFXSBQZXJmb3JtYW5jZSBpc3N1ZTogdGhpcyBtZXRob2QgbWF5IGNhbGxlZCBieSBldmVyeSBjaGFuZ2UgZGV0ZWN0aW9uc1xuICAvLyBUT0RPOiBjYWNoZSBtb3JlIGRlZXBseSBwYXRocyBmb3IgcGVyZm9ybWFuY2VcbiAgLyogdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueSAqL1xuICB0cmFuc2xhdGUocGF0aDogc3RyaW5nLCBkYXRhPzogYW55KTogc3RyaW5nIHtcbiAgICAvLyB0aGlzLl9sb2dnZXIuZGVidWcoYFtOekkxOG5TZXJ2aWNlXSBUcmFuc2xhdGluZygke3RoaXMuX2xvY2FsZS5sb2NhbGV9KTogJHtwYXRofWApO1xuICAgIGxldCBjb250ZW50ID0gdGhpcy5fZ2V0T2JqZWN0UGF0aCh0aGlzLl9sb2NhbGUsIHBhdGgpIGFzIHN0cmluZztcbiAgICBpZiAodHlwZW9mIGNvbnRlbnQgPT09ICdzdHJpbmcnKSB7XG4gICAgICBpZiAoZGF0YSkge1xuICAgICAgICBPYmplY3Qua2V5cyhkYXRhKS5mb3JFYWNoKGtleSA9PiAoY29udGVudCA9IGNvbnRlbnQucmVwbGFjZShuZXcgUmVnRXhwKGAlJHtrZXl9JWAsICdnJyksIGRhdGFba2V5XSkpKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBjb250ZW50O1xuICAgIH1cbiAgICByZXR1cm4gcGF0aDtcbiAgfVxuXG4gIC8qKlxuICAgKiBTZXQvQ2hhbmdlIGN1cnJlbnQgbG9jYWxlIGdsb2JhbGx5IHRocm91Z2hvdXQgdGhlIFdIT0xFIGFwcGxpY2F0aW9uXG4gICAqIFtOT1RFXSBJZiBjYWxsZWQgYXQgcnVudGltZSwgcmVuZGVyZWQgaW50ZXJmYWNlIG1heSBub3QgY2hhbmdlIGFsb25nIHdpdGggdGhlIGxvY2FsZSBjaGFuZ2UgKGJlY2F1c2UgdGhpcyBkbyBub3QgdHJpZ2dlciBhbm90aGVyIHJlbmRlciBzY2hlZHVsZSlcbiAgICogQHBhcmFtIGxvY2FsZSBUaGUgdHJhbnNsYXRpbmcgbGV0dGVyc1xuICAgKi9cbiAgc2V0TG9jYWxlKGxvY2FsZTogTnpJMThuSW50ZXJmYWNlKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuX2xvY2FsZSAmJiB0aGlzLl9sb2NhbGUubG9jYWxlID09PSBsb2NhbGUubG9jYWxlKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMuX2xvY2FsZSA9IGxvY2FsZTtcbiAgICB0aGlzLl9jaGFuZ2UubmV4dChsb2NhbGUpO1xuICB9XG5cbiAgZ2V0TG9jYWxlKCk6IE56STE4bkludGVyZmFjZSB7XG4gICAgcmV0dXJuIHRoaXMuX2xvY2FsZTtcbiAgfVxuXG4gIGdldExvY2FsZUlkKCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMuX2xvY2FsZSA/IHRoaXMuX2xvY2FsZS5sb2NhbGUgOiAnJztcbiAgfVxuXG4gIHNldERhdGVMb2NhbGUoZGF0ZUxvY2FsZTogRGF0ZUxvY2FsZSk6IHZvaWQge1xuICAgIHRoaXMuZGF0ZUxvY2FsZSA9IGRhdGVMb2NhbGU7XG4gIH1cblxuICBnZXREYXRlTG9jYWxlKCk6IERhdGVMb2NhbGUge1xuICAgIHJldHVybiB0aGlzLmRhdGVMb2NhbGU7XG4gIH1cblxuICAvKipcbiAgICogR2V0IGxvY2FsZSBkYXRhXG4gICAqIEBwYXJhbSBwYXRoIGRvdCBwYXRocyBmb3IgZmluZGluZyBleGlzdCB2YWx1ZSBmcm9tIGxvY2FsZSBkYXRhLCBlZy4gXCJhLmIuY1wiXG4gICAqIEBwYXJhbSBkZWZhdWx0VmFsdWUgZGVmYXVsdCB2YWx1ZSBpZiB0aGUgcmVzdWx0IGlzIG5vdCBcInRydXRoeVwiXG4gICAqL1xuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIGdldExvY2FsZURhdGEocGF0aD86IHN0cmluZywgZGVmYXVsdFZhbHVlPzogYW55KTogYW55IHtcbiAgICBjb25zdCByZXN1bHQgPSBwYXRoID8gdGhpcy5fZ2V0T2JqZWN0UGF0aCh0aGlzLl9sb2NhbGUsIHBhdGgpIDogdGhpcy5fbG9jYWxlO1xuICAgIHJldHVybiByZXN1bHQgfHwgZGVmYXVsdFZhbHVlO1xuICB9XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBwcml2YXRlIF9nZXRPYmplY3RQYXRoKG9iajogSW5kZXhhYmxlT2JqZWN0LCBwYXRoOiBzdHJpbmcpOiBzdHJpbmcgfCBvYmplY3QgfCBhbnkge1xuICAgIGxldCByZXMgPSBvYmo7XG4gICAgY29uc3QgcGF0aHMgPSBwYXRoLnNwbGl0KCcuJyk7XG4gICAgY29uc3QgZGVwdGggPSBwYXRocy5sZW5ndGg7XG4gICAgbGV0IGluZGV4ID0gMDtcbiAgICB3aGlsZSAocmVzICYmIGluZGV4IDwgZGVwdGgpIHtcbiAgICAgIHJlcyA9IHJlc1twYXRoc1tpbmRleCsrXV07XG4gICAgfVxuICAgIHJldHVybiBpbmRleCA9PT0gZGVwdGggPyByZXMgOiBudWxsO1xuICB9XG59XG4iXX0=